# dojo/aist/api.py
from __future__ import annotations

import uuid
from typing import Any

from django.db import transaction
from django.shortcuts import get_object_or_404
from rest_framework import serializers, status, generics
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated

from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample, OpenApiParameter

from .models import AISTPipeline, AISTProject, AISTStatus, AISTProjectVersion
from dojo.aist.tasks import run_sast_pipeline


class PipelineStartRequestSerializer(serializers.Serializer):
    """Minimal payload to start a pipeline."""
    aistproject_id = serializers.IntegerField(required=True)
    project_version = serializers.CharField(required=True)
    create_new_version_if_not_exist = serializers.BooleanField(required=True)


class PipelineStartResponseSerializer(serializers.Serializer):
    id = serializers.CharField()


class PipelineResponseSerializer(serializers.Serializer):
    id = serializers.CharField()
    status = serializers.CharField()
    response_from_ai = serializers.JSONField(allow_null=True)
    created = serializers.DateTimeField()
    updated = serializers.DateTimeField()


class PipelineStartAPI(APIView):
    """Start a new AIST pipeline."""
    permission_classes = [IsAuthenticated]

    @extend_schema(
        request=PipelineStartRequestSerializer,
        responses={
            201: OpenApiResponse(PipelineStartResponseSerializer, description="Pipeline created"),
            400: OpenApiResponse(description="Validation error"),
            404: OpenApiResponse(description="Project or project version not found"),
        },
        examples=[
            OpenApiExample(
                "Basic start",
                value={"aistproject_id": 42, "project_version": "master", "create_new_version_if_not_exist": True},
                request_only=True,
            )
        ],
        tags=["aist"],
        summary="Start pipeline",
        description=(
            "Creates and starts AIST Pipeline for the given AISTProject with default parameters."
        ),
    )
    def post(self, request, *args, **kwargs) -> Response:
        serializer = PipelineStartRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        project_id = serializer.validated_data["aistproject_id"]
        project = get_object_or_404(AISTProject, pk=project_id)

        project_version_hash = serializer.validated_data["project_version"]
        create_new_version = serializer.validated_data["create_new_version_if_not_exist"]

        if create_new_version:
            project_version, _created = AISTProjectVersion.objects.get_or_create(
                project=project, version=project_version_hash)
        else:
            project_version = get_object_or_404(AISTProjectVersion, project=project, version=project_version_hash)

        pipeline_id = uuid.uuid4().hex[:8]

        with transaction.atomic():
            p = AISTPipeline.objects.create(
                id=pipeline_id,
                project=project,
                project_version=project_version,
                status=AISTStatus.FINISHED,
            )

        async_result = run_sast_pipeline.delay(pipeline_id, None)
        p.run_sast_task_id = async_result.id
        p.save(update_fields=["run_task_id"])

        out = PipelineStartResponseSerializer({"id": pipeline_id})
        return Response(out.data, status=status.HTTP_201_CREATED)

class PipelineListAPI(generics.ListAPIView):
    """
    Paginated list of pipelines with simple filtering.
    """
    permission_classes = [IsAuthenticated]
    serializer_class = PipelineResponseSerializer

    @extend_schema(
        tags=["aist"],
        summary="List pipelines",
        description=(
            "Returns a paginated list of AIST pipelines. "
            "Filters: project_id, status, created_gte/lte (ISO8601). "
            "Ordering: created, -created, updated, -updated."
        ),
        parameters=[
            OpenApiParameter(name="project_id", location=OpenApiParameter.QUERY, description="Filter by AISTProject id", required=False, type=int),
            OpenApiParameter(name="status", location=OpenApiParameter.QUERY, description="Filter by status (string/choice)", required=False, type=str),
            OpenApiParameter(name="created_gte", location=OpenApiParameter.QUERY, description="Created >= (ISO8601)", required=False, type=str),
            OpenApiParameter(name="created_lte", location=OpenApiParameter.QUERY, description="Created <= (ISO8601)", required=False, type=str),
            OpenApiParameter(name="ordering", location=OpenApiParameter.QUERY, description="created | -created | updated | -updated", required=False, type=str),
            # Параметры пагинации из LimitOffsetPagination:
            OpenApiParameter(name="limit", location=OpenApiParameter.QUERY, required=False, type=int),
            OpenApiParameter(name="offset", location=OpenApiParameter.QUERY, required=False, type=int),
        ],
        responses={200: PipelineResponseSerializer(many=True)},
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        qs = (
            AISTPipeline.objects
            .select_related("project", "project_version")
            .all()
            .order_by("-created")
        )
        qp = self.request.query_params

        project_id = qp.get("project_id")
        status = qp.get("status")
        created_gte = qp.get("created_gte")
        created_lte = qp.get("created_lte")
        ordering = qp.get("ordering")

        if project_id:
            qs = qs.filter(project_id=project_id)
        if status:
            qs = qs.filter(status=status)
        if created_gte:
            qs = qs.filter(created__gte=created_gte)
        if created_lte:
            qs = qs.filter(created__lte=created_lte)
        if ordering in {"created", "-created", "updated", "-updated"}:
            qs = qs.order_by(ordering)

        return qs

class PipelineAPI(APIView):
    """Retrieve or delete a pipeline by id."""
    permission_classes = [IsAuthenticated]

    @extend_schema(
        responses={200: PipelineResponseSerializer, 404: OpenApiResponse(description="Not found")},
        tags=["aist"],
        summary="Get pipeline status",
        description="Returns pipeline status and AI response.",
    )
    def get(self, request, id: str, *args, **kwargs) -> Response:
        p = get_object_or_404(AISTPipeline, id=id)
        data = {
            "id": p.id,
            "status": p.status,
            "response_from_ai": p.response_from_ai,
            "created": p.created,
            "updated": p.updated,
        }
        out = PipelineResponseSerializer(data)
        return Response(out.data, status=status.HTTP_200_OK)

    @extend_schema(
        responses={204: OpenApiResponse(description="Pipeline deleted"),
                   400: OpenApiResponse(description="Cannot delete pipeline"),
                   404: OpenApiResponse(description="Not found")},
        tags=["aist"],
        summary="Delete pipeline",
        description="Deletes the specified AISTPipeline by id.",
    )
    def delete(self, request, id: str, *args, **kwargs) -> Response:
        p = get_object_or_404(AISTPipeline, id=id)
        if p.status != AISTStatus.FINISHED:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        p.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

class AISTProjectSerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source="product.name", read_only=True)

    class Meta:
        model = AISTProject
        fields = ["id", "product_name", "supported_languages", "compilable", "created", "updated"]


class AISTProjectAPI(generics.ListAPIView):
    """
    List all current AISTProjects.
    """
    queryset = AISTProject.objects.select_related("product").all().order_by("created")
    serializer_class = AISTProjectSerializer
    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["aist"],
        summary="List all AISTProjects",
        description="Returns all existing AISTProject records with their metadata."
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    @extend_schema(
        responses={204: OpenApiResponse(description="AIST project deleted"), 404: OpenApiResponse(description="Not found")},
        tags=["aist"],
        summary="Delete AIST project",
        description="Deletes the specified AISTProject by id.",
    )
    def delete(self, request, id: str, *args, **kwargs) -> Response:
        p = get_object_or_404(AISTProject, id=id)
        p.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)