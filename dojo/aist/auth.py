# dojo/aist/auth.py
from rest_framework.authentication import BaseAuthentication, get_authorization_header
from rest_framework import exceptions
from django.conf import settings
from django.contrib.auth.models import AnonymousUser

class CallbackUser(AnonymousUser):
    @property
    def is_authenticated(self):
        return True

class CallbackTokenAuthentication(BaseAuthentication):
    keyword = b"Bearer"

    def authenticate(self, request):
        auth = get_authorization_header(request).split()
        if not auth or auth[0].lower() != self.keyword.lower():
            return None  # нет заголовка — пусть другие схемы попробуют

        if len(auth) != 2:
            raise exceptions.AuthenticationFailed("Invalid Authorization header")

        token = auth[1].decode("utf-8")
        if token != getattr(settings, "CALLBACK_SECRET", ""):
            raise exceptions.AuthenticationFailed("Invalid token")

        return CallbackUser(), None
