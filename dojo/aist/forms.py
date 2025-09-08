from __future__ import annotations
from django import forms
from django.conf import settings
from .models import AISTProject, AISTProjectVersion
import json
from .utils import _load_analyzers_config


def _signature(project_id: str|None, langs: list[str], time_class: str|None) -> str:
    return f"{project_id or ''}::{time_class or 'slow'}::{','.join(sorted(set(langs or [])))}"

class AISTPipelineRunForm(forms.Form):
    project = forms.ModelChoiceField(
        queryset=AISTProject.objects.all(),
        label="Project",
        help_text="Choose a pre-configured SAST project",
        required=True,
    )
    project_version = forms.ModelChoiceField(
        queryset=AISTProjectVersion.objects.none(),  # заполним позже в __init__
        label="Project version",
        required=False,
        help_text="By default will be used latest commit on master branch",
    )
    ask_push_to_ai = forms.BooleanField(required=True, initial=True, label="Ask for confirmation before pushing to AI")
    rebuild_images = forms.BooleanField(required=False, initial=False, label="Rebuild images")
    log_level = forms.ChoiceField(
        choices=[("INFO","INFO"),("DEBUG","DEBUG"),("WARNING","WARNING"),("ERROR","ERROR")],
        initial="INFO",
        label="Log level",
    )
    languages = forms.MultipleChoiceField(choices=[], required=False, label="Languages", widget=forms.CheckboxSelectMultiple)
    analyzers = forms.MultipleChoiceField(choices=[], required=False, label="Analyzers to launch", widget=forms.CheckboxSelectMultiple)
    time_class_level = forms.ChoiceField(choices=[], required=False, label="Maximum time class", initial="medium")
    selection_signature = forms.CharField(required=False, widget=forms.HiddenInput)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Apply Bootstrap classes for consistent look & feel
        self.fields["project"].widget.attrs.update({"class": "form-select"})
        self.fields["log_level"].widget.attrs.update({"class": "form-select"})
        self.fields["time_class_level"].widget.attrs.update({"class": "form-select"})
        self.fields["rebuild_images"].widget.attrs.update({"class": "form-check-input"})
        # CheckboxSelectMultiple: class will be applied to each checkbox input
        self.fields["languages"].widget.attrs.update({"class": "form-check-input"})
        self.fields["analyzers"].widget.attrs.update({"class": "form-check-input"})
        self.fields["project_version"].widget.attrs.update({"class": "form-select"})
        self.fields["project_version"].empty_label = "Use default (latest on default branch)"
        self.fields["project_version"].queryset = AISTProjectVersion.objects.none()

        cfg = _load_analyzers_config()
        if cfg:
            self.fields["languages"].choices = [(x, x) for x in cfg.get_supported_languages()]
            self.fields["analyzers"].choices = [(x, x) for x in cfg.get_supported_analyzers()]
            self.fields["time_class_level"].choices = [(x, x) for x in cfg.get_analyzers_time_class()]

        if not self.is_bound:
            return

        project_id = self.data.get(self.add_prefix("project")) or None
        if project_id:
            try:
                proj = AISTProject.objects.get(id=project_id)
                self.fields["project_version"].queryset = proj.versions.all()
            except AISTProject.DoesNotExist:
                pass

        posted_langs = self.data.getlist(self.add_prefix("languages"))
        project_supported_languages = (proj.supported_languages if proj else []) or []
        langs_union = list(set((posted_langs or []) + project_supported_languages))

        time_class = self.data.get(self.add_prefix("time_class_level")) or "slow"

        posted_sig = self.data.get(self.add_prefix("selection_signature")) or ""
        new_sig = _signature(project_id, langs_union, time_class)
        self.initial["selection_signature"] = new_sig

        defaults = []
        if cfg:
            non_compile_project = not proj.compilable
            filtered = cfg.get_filtered_analyzers(
                analyzers_to_run=None,
                max_time_class=time_class,
                non_compile_project=non_compile_project,
                target_languages=langs_union,
                show_only_parent=True
            )
            defaults = cfg.get_names(filtered)

        if posted_sig != new_sig:
            qd = self.data.copy()
            qd.setlist(self.add_prefix("analyzers"), defaults)
            self.data = qd
            self.initial["analyzers"] = defaults
        else:
            self.initial["analyzers"] = self.data.getlist(self.add_prefix("analyzers")) or defaults

    def get_params(self) -> dict:
        """Collect final CLI/runner parameters from the selected SASTProject and form options."""
        proj: AISTProject = self.cleaned_data["project"]
        pv: AISTProjectVersion | None = self.cleaned_data.get("project_version")

        if not pv:
            pv = proj.versions.order_by("-created").first()

        return dict(
            # from project model (immutable in the form)
            project_name=proj.product.name,
            script_path=proj.script_path,
            project_version=(pv.id if pv else None),
            supported_languages=proj.supported_languages,
            # from user options
            rebuild_images=self.cleaned_data.get("rebuild_images") or False,
            log_level=self.cleaned_data.get("log_level") or "INFO",
            languages=self.cleaned_data.get("languages") or [],
            analyzers=self.cleaned_data.get("analyzers") or [],
            time_class_level=self.cleaned_data.get("time_class_level"),
            ask_push_to_ai=self.cleaned_data.get("ask_push_to_ai") or True,
        )
