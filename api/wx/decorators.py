from functools import wraps
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import redirect
from django.urls import reverse

from wx.permissions import user_can


def _wants_json(request) -> bool:
    # If frontend sends this header (many setups do)
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True

    # If client explicitly accepts JSON
    accept = request.headers.get("Accept", "")
    if "application/json" in accept:
        return True

    return False


def wx_permission_required(page_url_name: str, action: str):
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):

            # Not logged in
            if not request.user.is_authenticated:
                if _wants_json(request):
                    return JsonResponse({"detail": "Authentication required."}, status=401)
                login_url = getattr(settings, "LOGIN_URL", "/accounts/login/")
                return redirect(f"{login_url}?next={request.get_full_path()}")

            # Not authorized
            if not user_can(request.user, page_url_name, action):
                if _wants_json(request):
                    return JsonResponse({"detail": "Not authorized."}, status=403)
                return redirect(reverse("not-auth"))

            return view_func(request, *args, **kwargs)

        return _wrapped
    return decorator
