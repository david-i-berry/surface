"""
permissions_map.py

This module documents and centralizes the SURFACE Wx permissions system.

Why this file exists
--------------------
- Document how permissions work for current/future developers.
- Provide a single place to map helper/AJAX endpoints to their parent UI pages.
- Reduce duplication and mistakes (parent/action copied incorrectly across many views).

Core idea
---------
We treat permissions as "page access" with three actions:

    read   -> view the page + fetch data used by the page
    write  -> create/update/upload/process data used by the page
    delete -> delete records/files belonging to the page

Permissions are stored in WxGroupPageAccess:
- Each row links a Django Group -> a WxPermissionPages entry (url_name) with can_read/can_write/can_delete flags.
- Rules enforced:
    * write/delete requires read
    * at least one permission must be True (no all-false rows)

-------------------------------------------------------------------------------
1) UI PAGES ("Pages")
-------------------------------------------------------------------------------
Definition:
- A "page" is a route that renders a full HTML template / screen.
- Typically a class-based view using .as_view() and a named URL pattern.

How it is secured:
- Pages use WxPermissionRequiredMixin.
- The mixin auto-detects the current route name (url_name) and enforces permissions.
- Pages require at least "read" to view (simply by nature of the call).

Example:
    # Important: the mixin must come first in the inheritance list.
    class SpatialAnalysisView(WxPermissionRequiredMixin, LoginRequiredMixin, TemplateView):
        template_name = "wx/spatial_analysis.html"

-------------------------------------------------------------------------------
2) HELPER / AJAX ENDPOINTS ("Helpers")
-------------------------------------------------------------------------------
Definition:
- A helper endpoint is NOT a page.
- It returns JSON/files or performs actions called by a page (Axios/fetch).
- These endpoints are often function views and may be CSRF exempt.

How it is secured:
We support two styles:

A) MAPPED (preferred default)
- Use @wx_mapped_permission_required
- The decorator resolves the endpoint url_name and looks it up in ENDPOINT_PARENT_PAGE
- This forces developers to keep ENDPOINT_PARENT_PAGE updated

B) MANUAL OVERRIDE (exceptions)
- Use @wx_permission_required("<parent_page>", "<action>")
- Bypasses ENDPOINT_PARENT_PAGE
- Use when an endpoint is shared or has special rules

-------------------------------------------------------------------------------
ACTION RULES (how we decide read/write/delete)
-------------------------------------------------------------------------------
Typical mapping by HTTP method:
- GET / HEAD / OPTIONS -> read
- POST / PUT / PATCH   -> write
- DELETE               -> delete

Note:
- Many Django apps do not use HTTP DELETE.
- Endpoints that "delete things" should still be treated as action="delete"
  even if the request method is POST/GET.

-------------------------------------------------------------------------------
FRONTEND HIDING (UI/UX layer)
-------------------------------------------------------------------------------
Backend enforcement is the security layer (required).
Frontend hiding is UX polish.

In Django templates we hide buttons/menus based on "page:action" permissions.

Format:
    "<url_name>:<action>"

Examples:
    {% if request|has_any_feature_permission:"manual-data-import:write" %}
        <button>Upload</button>
    {% endif %}

    {% if request|has_any_feature_permission:"manual-data-import:delete" %}
        <button>Delete</button>
    {% endif %}

Superuser-only UI:
    {% if request.user.is_superuser %}
        <button>Admin Only</button>
    {% endif %}

-------------------------------------------------------------------------------
ROLL-OUT ORDER (important)
-------------------------------------------------------------------------------
Apply permissions in this order to avoid breaking pages:

1) Secure the page view (mixin) first
2) Secure helper endpoints used by the page (decorators)
3) Hide UI buttons/options for that page
4) Move on to the next page

-------------------------------------------------------------------------------
GOTCHAS / REMINDERS
-------------------------------------------------------------------------------
- Some pages call endpoints located in other modules — secure those too.
- If an endpoint is shared by multiple pages:
    * choose a primary parent page permission OR
    * split the endpoint into specific endpoints for each page
- Downloads:
    * use read for safe exports
    * use write if the export is sensitive or "generates" data
- manage-permissions should remain superuser-only unless explicitly designed otherwise.

-------------------------------------------------------------------------------
ENDPOINT → PARENT PAGE PERMISSION MAP
-------------------------------------------------------------------------------
This mapping documents which permission each helper endpoint inherits.

Format:
    "<endpoint_url_name>": ("<parent_page_url_name>", "<action>")

Strict mode:
- If settings.WX_PERMISSIONS_STRICT_MAP is True, missing entries should raise loudly
  so developers are forced to update this file.
"""






# NOTE:
# Only endpoints protected with @wx_mapped_permission_required must appear here.
# If an endpoint uses manual @wx_permission_required(...) it does not need mapping.

ENDPOINT_PARENT_PAGE = {
    # -------------------------------------------------------------------------
    # Manual Data Import Page (manual-data-import)
    # -------------------------------------------------------------------------
    # Page:
    #   manual-data-import (GET page)
    # Helpers:
    #   list uploaded files, validate imports, upload imports, remove/delete files

    "manual-data-files": ("manual-data-import", "read"),
    "check-manual-import": ("manual-data-import", "write"),
    "upload-manual-data-file": ("manual-data-import", "write"),
    "remove-manual-data-file": ("manual-data-import", "read"),
    "data-import-manual-delete": ("manual-data-import", "delete"),

    # # -------------------------------------------------------------------------
    # # Spatial Analysis Page (spatial-analysis)
    # # -------------------------------------------------------------------------
    # # Page:
    # #   spatial-analysis (GET page)
    # # Helpers:
    # #   fetch data/image, request interpolation, colorbar, etc.

    # "spatial-analysis-data": ("spatial-analysis", "read"),
    # "spatial-analysis-image": ("spatial-analysis", "read"),
    # "spatial-analysis-interpolate_data": ("spatial-analysis", "write"),
    # "spatial-analysis-color-bar": ("spatial-analysis", "read"),
    # "spatial-analysis-get-image": ("spatial-analysis", "read"),

    # # Add more mappings below...
}

from django.conf import settings

def get_parent_permission(endpoint_url_name: str):
    """
    Return (parent_page, action) for the endpoint url_name if defined.

    If WX_PERMISSIONS_STRICT_MAP is True:
    - raise an error when an endpoint is missing from the map
      (helps catch omissions during development)
    """
    perm = ENDPOINT_PARENT_PAGE.get(endpoint_url_name)

    if perm is None and getattr(settings, "WX_PERMISSIONS_STRICT_MAP", False):
        raise KeyError(
            f"Endpoint '{endpoint_url_name}' missing from ENDPOINT_PARENT_PAGE in permissions_map.py"
        )

    return perm
