from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.urls.exceptions import NoReverseMatch
from rest_framework.exceptions import AuthenticationFailed


class NotFilledException(TypeError, ValueError, ValidationError, Exception):
    pass


class NotFoundException(AttributeError, ObjectDoesNotExist, NoReverseMatch, Exception):
    pass


class IncorrectPasswordException(AuthenticationFailed):
    pass
