from __future__ import annotations
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.contrib import messages
from django.db import transaction
from .forms import AISTPipelineRunForm
from .models import AISTPipeline, AISTProject, AISTStatus
from .tasks import run_sast_pipeline
import uuid
from typing import Any, Dict

def start_pipeline(request):
    # If there is a non-finished pipeline, redirect to its status
    active = AISTPipeline.objects.exclude(status=AISTStatus.FINISHED).first()
    if active:
        return redirect('dojo_aist:pipeline_detail', pk=active.pk)

    if request.method == "POST":
        form = AISTPipelineRunForm(request.POST)
        if form.is_valid():
            params = form.get_params()
            # align ID with PIPELINE_ID logic (8 hex is fine)
            pipeline_id = uuid.uuid4().hex[:8]
            with transaction.atomic():
                p = AISTPipeline.objects.create(id=pipeline_id, project=form.cleaned_data["project"], status=AISTStatus.SAST_LAUNCHED)
            run_sast_pipeline.delay(pipeline_id, params)
            messages.success(request, "SAST pipeline launched")
            return redirect('dojo_aist:pipeline_detail', id=pipeline_id)
    else:
        form = AISTPipelineRunForm()

    return render(request, 'dojo/aist/start.html', {'form': form})

def pipeline_detail(request, id: int):
    """Display the status and logs of a specific SAST pipeline."""
    pipeline = get_object_or_404(AISTPipeline, id=id)
    # Determine which template fragment to show based on the current status
    context: Dict[str, Any] = {'pipeline': pipeline}
    return render(request, 'dojo/aist/pipeline_detail.html', context)
