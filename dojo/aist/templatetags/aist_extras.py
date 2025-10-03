from django import template
import json

register = template.Library()

@register.filter
def to_pretty_json(value):
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        return '(unserializable)'
