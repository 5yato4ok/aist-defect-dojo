from __future__ import annotations

import time
import uuid
from typing import Any, Dict
from django.core.paginator import Paginator
from django.db.models import Q


from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpResponse, StreamingHttpResponse, HttpResponseForbidden, Http404
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, StreamingHttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from dojo.deduplication_tracker import TestDeduplicationProgress
from django.db.models import Count
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from dojo.utils import add_breadcrumb
from django.views.decorators.http import require_POST
from django.http import JsonResponse, HttpResponseBadRequest
from .tasks import run_sast_pipeline, send_request_to_ai
from .forms import AISTPipelineRunForm , _load_analyzers_config, _signature # type: ignore


from .utils import stop_pipeline, _fmt_duration, _qs_without, _install_db_logging
from .auth import CallbackTokenAuthentication
import time
from django.http import StreamingHttpResponse, Http404
from django.db import close_old_connections
from django.utils.timezone import now

from .models import AISTPipeline, AISTStatus, AISTProject

@require_POST
def aist_default_analyzers(request):
    project_id = request.POST.get("project")
    time_class = request.POST.get("time_class_level") or "slow"
    langs = request.POST.getlist("languages") or request.POST.getlist("languages[]")

    proj = AISTProject.objects.filter(id=project_id).first()
    proj_langs = (proj.supported_languages if proj else []) or []
    langs_union = list(set((langs or []) + proj_langs))

    cfg = _load_analyzers_config()
    if not cfg:
      return HttpResponseBadRequest("config not loaded")

    filtered = cfg.get_filtered_analyzers(
        analyzers_to_run=None,
        max_time_class=time_class,
        non_compile_project=bool(proj and not proj.compilable),
        target_languages=langs_union,
        show_only_parent=True
    )
    defaults = cfg.get_names(filtered)

    return JsonResponse({
        "defaults": defaults,
        "signature": _signature(project_id, langs_union, time_class),
    })

@api_view(["POST"])
@authentication_classes([CallbackTokenAuthentication])
@permission_classes([IsAuthenticated])
def pipeline_callback(request, id: int):
    try:
        get_object_or_404(AISTPipeline, id=str(id))
        response_from_ai = request.data
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

    errors = response_from_ai.pop("errors", None)
    logger = _install_db_logging(str(id))
    if errors:
        logger.error(errors)

    with transaction.atomic():
        pipeline = (
            AISTPipeline.objects
            .select_for_update()
            .get(id=id)
        )
        pipeline.response_from_ai = response_from_ai
        pipeline.status = AISTStatus.FINISHED
        pipeline.save(update_fields=["status", "response_from_ai", "updated"])

    return Response({"ok": True})

def pipeline_status_stream(request, id: str):
    """
    SSE endpoint: шлёт событие 'status' при смене статуса пайплайна.
    Завершается событием 'done' при FINISHED/FAILED/DELETED.
    """
    # Быстрая проверка, что объект вообще существует на момент старта
    if not AISTPipeline.objects.filter(id=id).exists():
        raise Http404("Pipeline not found")

    def event_stream():
        last_status = None
        heartbeat_every = 3  # сек, раз в N секунд шлём keep-alive комментарий
        last_heartbeat = 0.0

        try:
            while True:
                # В стримах важно периодически закрывать/переподключать старые коннекты
                close_old_connections()

                # Заново читаем объект каждый цикл — не храним «живой» инстанс
                obj = (
                    AISTPipeline.objects
                    .only("id", "status", "updated")
                    .filter(id=id)
                    .first()
                )

                if obj is None:
                    # Объект удалён — сообщаем клиенту и выходим
                    yield "event: done\ndata: deleted\n\n"
                    break

                if obj.status != last_status:
                    last_status = obj.status
                    # Правильный блок SSE: сначала event, затем data, затем пустая строка
                    yield f"event: status\ndata: {last_status}\n\n"

                    if last_status in (
                        getattr(AISTStatus, "FINISHED", "FINISHED"),
                        getattr(AISTStatus, "FAILED", "FAILED"),
                    ):
                        yield "event: done\ndata: finished\n\n"
                        break

                # Heartbeat, чтобы прокси (например, Nginx) не закрывали соединение
                now_ts = time.time()
                if now_ts - last_heartbeat >= heartbeat_every:
                    last_heartbeat = now_ts
                    yield f": heartbeat {int(now_ts)}\n\n"  # комментарий по спецификации SSE

                time.sleep(1)

        except GeneratorExit:
            # Клиент закрыл соединение — просто выходим, без исключений в логах
            return
        finally:
            close_old_connections()

    resp = StreamingHttpResponse(event_stream(), content_type="text/event-stream")
    resp["Cache-Control"] = "no-cache"
    resp["X-Accel-Buffering"] = "no"  # важно для Nginx, чтобы не буферил стрим
    return resp

def push_to_ai(request, id:str):
    add_breadcrumb(title="Push to AI", request=request)

    if not AISTPipeline.objects.filter(id=id).exists():
        raise Http404("Pipeline not found")

    logger = _install_db_logging(str(id))
    if request.method == "POST":

        with transaction.atomic():
            pipeline = (
                AISTPipeline.objects
                .select_for_update()
                .get(id=id)
            )
            if pipeline.status != AISTStatus.WAITING_CONFIRMATION_TO_PUSH_TO_AI:
                logger.error("Attempt to push to AI before receiving confirmation")
                return JsonResponse({"error": "Attempt to push to AI before receiving confirmation"}, status=400)

            pipeline.status = AISTStatus.PUSH_TO_AI
            pipeline.save(update_fields=["status", "updated"])
        send_request_to_ai.delay(pipeline.id, "INFO")

    return redirect('dojo_aist:pipeline_detail', id=id)


def start_pipeline(request: HttpRequest) -> HttpResponse:
    """Launch a new SAST pipeline or redirect to the active one.

    If there is an existing pipeline that hasn't finished yet the user
    is redirected to its detail page.  Otherwise this view presents a
    form allowing the user to configure and start a new pipeline.  On
    successful submission a new pipeline is created and the Celery
    task is triggered.
    """
    # If there is an active pipeline that hasn't finished yet,
    # immediately redirect the user to its status page.  Only a single
    # pipeline may run at a time.
    active = AISTPipeline.objects.exclude(status=AISTStatus.FINISHED).first()
    if active:
        return redirect('dojo_aist:pipeline_detail', id=active.id)

    project_id = request.GET.get("project")
    q = (request.GET.get("q") or "").strip()

    history_qs = (
        AISTPipeline.objects
        .filter(status=AISTStatus.FINISHED)
        .select_related("project__product")
    )
    if project_id:
        history_qs = history_qs.filter(project_id=project_id)
    if q:
        history_qs = history_qs.filter(
            Q(id__icontains=q) |
            Q(project__product__name__icontains=q)
        )

    history_qs = history_qs.order_by("-updated")


    per_page = int(request.GET.get("page_size") or 8)
    paginator = Paginator(history_qs, per_page)
    page_obj = paginator.get_page(request.GET.get("page") or 1)

    history_items = [{
        "id": p.id,
        "project_name": getattr(getattr(p.project, "product", None), "name", str(p.project_id)),
        "updated": p.updated,
        "status": p.status,
        "duration": _fmt_duration(p.created, p.updated),
    } for p in page_obj.object_list]

    history_qs_str = _qs_without(request, "page")
    add_breadcrumb( title="Start pipeline", top_level=True, request=request)

    if request.method == "POST":
        form = AISTPipelineRunForm(request.POST)
        if form.is_valid():
            params = form.get_params()
            # Generate a unique identifier for this pipeline.  The
            # downstream SAST pipeline uses an 8 character hex string
            # internally, so mirror that here.

            pipeline_id = uuid.uuid4().hex[:8]
            with transaction.atomic():
                p = AISTPipeline.objects.create(
                    id=pipeline_id,
                    project=form.cleaned_data["project"],
                    status=AISTStatus.FINISHED,
                )
            # Launch the Celery task and record its id on the pipeline.
            # Storing the task id allows us to revoke it later if the
            # user chooses to stop the pipeline.
            async_result = run_sast_pipeline.delay(pipeline_id, params)
            p.run_sast_task_id = async_result.id
            p.save(update_fields=["run_task_id"])
            return redirect('dojo_aist:pipeline_detail', id=pipeline_id)
    else:
        form = AISTPipelineRunForm()
    return render(request, 'dojo/aist/start.html', {'form': form, 'history_page': page_obj,  # для пагинации
                                                    'history_items': history_items,
                                                    'history_qs': history_qs_str,
                                                    'selected_project': project_id or "",
                                                    'search_query': q, "page_sizes": [10, 20, 50, 100]})

def pipeline_list(request):
    project_id = request.GET.get("project")
    q = (request.GET.get("q") or "").strip()
    status = (request.GET.get("status") or "FINISHED").upper()  # FINISHED | ALL
    per_page = int(request.GET.get("page_size") or 20)

    qs = (AISTPipeline.objects
          .select_related("project__product")
          .order_by("-updated"))
    if status != "ALL":
        qs = qs.filter(status=AISTStatus.FINISHED)

    if project_id:
        qs = qs.filter(project_id=project_id)
    if q:
        qs = qs.filter(
            Q(id__icontains=q) |
            Q(project__product__name__icontains=q)
        )

    paginator = Paginator(qs, per_page)
    page_obj = paginator.get_page(request.GET.get("page") or 1)

    items = [{
        "id": p.id,
        "project_name": getattr(getattr(p.project, "product", None), "name", str(p.project_id)),
        "created": p.created,
        "updated": p.updated,
        "status": p.status,
        "duration": _fmt_duration(p.created, p.updated),
    } for p in page_obj.object_list]

    qs_str = _qs_without(request, "page")

    projects = AISTProject.objects.select_related("product").order_by("product__name")
    add_breadcrumb(title="Pipeline History", top_level=True, request=request)
    return render(
        request,
        'dojo/aist/pipeline_list.html',
        {
            "page_obj": page_obj,
            "items": items,
            "qs": qs_str,
            "selected_project": project_id or "",
            "search_query": q,
            "status": status,
            "projects": projects,
        }
    )


def pipeline_detail(request, id: str):
    """
    Display the status and logs for a pipeline.
    Adds actions (Stop/Delete) and connects SSE client to stream logs.
    """
    pipeline = get_object_or_404(AISTPipeline, id=id)
    if request.headers.get("X-Partial") == "status":
        return render(request, "dojo/aist/_pipeline_status_container.html", {"pipeline": pipeline})

    add_breadcrumb(parent=pipeline, title="Pipeline Detail", top_level=False, request=request)
    return render(request, "dojo/aist/pipeline_detail.html", {"pipeline": pipeline})


@login_required
@require_http_methods(["POST"])
def stop_pipeline_view(request, id: str):
    """
    POST-only endpoint to stop a running pipeline (Celery revoke).
    Sets FINISHED regardless of current state to keep UI consistent.
    """
    pipeline = get_object_or_404(AISTPipeline, id=id)
    with transaction.atomic():
        stop_pipeline(pipeline)
    pipeline.save(update_fields=["status"])
    return redirect("dojo_aist:pipeline_detail", id=pipeline.id)

@login_required
@require_http_methods(["GET", "POST"])
def delete_pipeline_view(request, id: str):
    """
    Delete a pipeline after confirmation (POST). GET returns a confirm view.
    """
    pipeline = get_object_or_404(AISTPipeline, id=id)
    if request.method == "POST":
        pipeline.delete()
        return redirect("dojo_aist:start_pipeline")
    add_breadcrumb(parent=pipeline, title="Delete pipeline", top_level=False, request=request)
    return render(request, "dojo/aist/confirm_delete.html", {"pipeline": pipeline})


@csrf_exempt  # SSE does not need CSRF for GET; keep it simple
@require_http_methods(["GET"])
def stream_logs_sse(request, id: str):
    """
    Server-Sent Events endpoint that streams new log lines for a pipeline.
    Reads from DB; emits only new tail bytes every poll tick.
    """
    pipeline = get_object_or_404(AISTPipeline, id=id)

    def event_stream():
        last_len = 0
        # Simple polling loop. Replace with channels/redis pub-sub if desired.
        for _ in range(60 * 60 * 12):  # up to ~12h
            p = AISTPipeline.objects.filter(id=pipeline.id).only("logs", "status").first()
            if not p:
                break
            data = p.logs or ""
            if len(data) > last_len:
                chunk = data[last_len:]
                last_len = len(data)
                # SSE frame
                yield f"data: {chunk}\n\n"
            if p.status == AISTStatus.FINISHED:
                yield "event: done\ndata: FINISHED\n\n"
                break
            time.sleep(0.3)

    resp = StreamingHttpResponse(event_stream(), content_type="text/event-stream; charset=utf-8")
    resp["Cache-Control"] = "no-cache, no-transform"
    resp["X-Accel-Buffering"] = "no"
    return resp

def pipeline_progress_json(request, id: str):
    """
    Return deduplication progress for a pipeline as JSON (per Test and overall).
    Progress counts findings, not just tests with a boolean flag.
    """
    pipeline = get_object_or_404(AISTPipeline, id=id)

    tests = (
        pipeline.tests
        .select_related("engagement")
        .annotate(total_findings=Count("finding", distinct=True))
        .order_by("id")
    )

    tests_payload = []
    overall_total = 0
    overall_processed = 0

    for t in tests:
        # Ensure progress row exists and is refreshed if needed
        prog, _ = TestDeduplicationProgress.objects.get_or_create(test=t)
        # `pending_tasks` = findings total - processed; we keep `refresh_pending_tasks()` the SSOT.
        prog.refresh_pending_tasks()

        total = getattr(t, "total_findings", 0)
        pending = prog.pending_tasks
        processed = max(total - pending, 0)
        pct = 100 if total == 0 else int(processed * 100 / total)

        overall_total += total
        overall_processed += processed

        tests_payload.append({
            "test_id": t.id,
            "test_name": getattr(t, "title", None) or f"Test #{t.id}",
            "total_findings": total,
            "processed": processed,
            "pending": pending,
            "percent": pct,
            "completed": bool(prog.deduplication_complete),
        })

    overall_pct = 100 if overall_total == 0 else int(overall_processed * 100 / overall_total)

    return JsonResponse({
        "status": pipeline.status,
        "overall": {
            "total_findings": overall_total,
            "processed": overall_processed,
            "pending": max(overall_total - overall_processed, 0),
            "percent": overall_pct,
        },
        "tests": tests_payload,
    })
