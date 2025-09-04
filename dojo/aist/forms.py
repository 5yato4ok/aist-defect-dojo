from __future__ import annotations
from django import forms
from django.conf import settings
from .models import AISTProject

def _load_analyzers_config():
    import importlib, sys, os
    code_path = getattr(settings, "AIST_PIPELINE_CODE_PATH", None)
    if code_path and os.path.isdir(code_path):
        if code_path not in sys.path:
            sys.path.insert(0, code_path)
        try:
            return importlib.import_module("pipeline.config_utils").AnalyzersConfigHelper()
        except Exception:
            return None
    return None

class AISTPipelineRunForm(forms.Form):
    project = forms.ModelChoiceField(
        queryset=AISTProject.objects.all(),
        label="Project",
        help_text="Choose a pre-configured SAST project",
        required=True,
    )
    rebuild_images = forms.BooleanField(required=False, initial=False, label="Rebuild images")
    log_level = forms.ChoiceField(
        choices=[("INFO","INFO"),("DEBUG","DEBUG"),("WARNING","WARNING"),("ERROR","ERROR")],
        initial="INFO",
        label="Log level",
    )
    languages = forms.MultipleChoiceField(choices=[], required=False, label="Languages", widget=forms.CheckboxSelectMultiple)
    analyzers = forms.MultipleChoiceField(choices=[], required=False, label="Specific analyzers to launch", widget=forms.CheckboxSelectMultiple)
    time_class_level = forms.ChoiceField(choices=[], required=False, label="Analyzers time class")

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

        cfg = _load_analyzers_config()
        if cfg:
            self.fields["languages"].choices = [(x, x) for x in cfg.get_supported_languages()]
            self.fields["analyzers"].choices = [(x, x) for x in cfg.get_supported_analyzers()]
            self.fields["time_class_level"].choices = [(x, x) for x in cfg.get_analyzers_time_class()]

    def get_params(self) -> dict:
        """Collect final CLI/runner parameters from the selected SASTProject and form options."""
        proj: AISTProject = self.cleaned_data["project"]
        return dict(
            # from project model (immutable in the form)
            project_name=proj.product.name,
            script_path=proj.script_path,
            project_path=proj.project_path,
            project_version=proj.project_version,
            supported_languages=proj.supported_languages,
            output_dir=proj.output_dir,
            # from user options
            rebuild_images=self.cleaned_data.get("rebuild_images") or False,
            log_level=self.cleaned_data.get("log_level") or "INFO",
            languages=self.cleaned_data.get("languages") or [],
            analyzers=self.cleaned_data.get("analyzers") or [],
            time_class_level=self.cleaned_data.get("time_class_level"),
        )
