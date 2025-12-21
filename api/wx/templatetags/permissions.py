from django import template

register = template.Library()

@register.filter
def has_any_feature_permission(request, features):
    """
    Frontend-only feature gate.

    Expects:
    - request.user_permissions to be an iterable of strings
    """

    # Defensive check:
    # - request.user may not exist if AuthenticationMiddleware is missing
    # - anonymous users should never see frontend-restricted features
    if not hasattr(request, "user") or not request.user.is_authenticated:
        return False

    # Superusers see everything in the UI
    if request.user.is_superuser:
        return True

    # If frontend permissions were not attached to the request
    if not hasattr(request, "user_permissions"):
        return False

    feature_list = [f.strip() for f in features.split(",")]

    return any(f in request.user_permissions for f in feature_list)