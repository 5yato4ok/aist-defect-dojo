from __future__ import annotations

import time
from typing import Any, Dict

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpResponse, StreamingHttpResponse, HttpResponseForbidden, Http404
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from .tasks import run_sast_pipeline
from .forms import AISTPipelineRunForm  # type: ignore

from .models import AISTPipeline, AISTStatus
from .utils import DatabaseLogHandler, stop_pipeline

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

    if request.method == "POST":
        form = AISTPipelineRunForm(request.POST)
        if form.is_valid():
            params = form.get_params()
            # Generate a unique identifier for this pipeline.  The
            # downstream SAST pipeline uses an 8 character hex string
            # internally, so mirror that here.
            import uuid
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
            messages.success(request, "SAST pipeline launched")
            return redirect('dojo_aist:pipeline_detail', id=pipeline_id)
    else:
        form = AISTPipelineRunForm()
    return render(request, 'dojo/aist/start.html', {'form': form})

def pipeline_detail(request, id: str):
    """
    Display the status and logs for a pipeline.
    Adds actions (Stop/Delete) and connects SSE client to stream logs.
    """
    pipeline = get_object_or_404(AISTPipeline, id=id)
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
    messages.info(request, "Pipeline stopped successfully.")
    messages.success(request, "Pipeline was stopped and marked as FINISHED.")
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
        messages.success(request, "Pipeline deleted.")
        return redirect("dojo_aist:start_pipeline")
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
