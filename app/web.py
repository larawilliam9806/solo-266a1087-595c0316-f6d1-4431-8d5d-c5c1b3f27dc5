"""Flask 报送页：报送/换报、撤回、合并摘要下载、带凭据号下载与按号查证。"""

from __future__ import annotations

import json
import re
import os
from typing import Any

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from .db import Database
from .errors import NotFoundError, ValidationError
from .hll import validate_position

_MONTH_SEGMENT_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# 查询参数里凭据号 / 位置可能出现的异名。
_CREDENTIAL_KEYS = (
    "credential_id",
    "credential_no",
    "credential_number",
    "cid",
    "id",
    "credential",
    "certificate_id",
    "cert_id",
    "certificate_no",
    "cert_no",
    "certificate",
    "cert",
    "receipt_id",
    "receipt_no",
    "voucher_id",
    "voucher_no",
    "evidence_id",
    "proof_id",
    "token",
    "serial",
    "serial_no",
    "no",
    "number",
    "code",
)
_POSITION_KEYS = (
    "position",
    "pos",
    "index",
    "register",
    "register_index",
    "p",
)
# /credentials/<this> 不能当成凭据号。
_RESERVED_WORDS = {
    "lookup",
    "verify",
    "query",
    "issue",
    "new",
    "create",
    "download",
    "list",
}


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path or os.environ.get(
        "HLL_DB_PATH", "/data/hll.sqlite3"
    )
    app.config["DATABASE"] = Database(app.config["DB_PATH"])

    def get_db() -> Database:
        return app.config["DATABASE"]

    # -- 入参解析 --------------------------------------------------------

    def _wants_html() -> bool:
        """浏览器表单提交走 HTML 跳转，JSON/API 调用一律走 JSON 响应。"""
        if request.path.endswith("/download"):
            return False
        # 只有浏览器表单（urlencoded / multipart）才走页面跳转；
        # application/json 及其他程序化调用（含 Accept: */* 的客户端）返回 JSON。
        return request.content_type is not None and (
            request.content_type.startswith("application/x-www-form-urlencoded")
            or request.content_type.startswith("multipart/form-data")
        )

    def _browser_accepts_html() -> bool:
        """GET 查证页的内容协商：浏览器导航给 HTML，程序化调用给 JSON。"""
        if request.args.get("format") == "json":
            return False
        if request.args.get("format") == "html":
            return True
        accept = request.headers.get("Accept", "")
        return "text/html" in accept and "application/json" not in accept

    def _payload() -> dict[str, Any]:
        """同时支持 application/json、multipart/form-data 与 JSON 文件上传。"""
        if request.is_json:
            data = request.get_json(silent=False)
            if not isinstance(data, dict):
                raise ValidationError("JSON 请求体必须是对象")
            return data

        data: dict[str, Any] = {}
        if request.form:
            data.update(request.form.to_dict(flat=True))
        upload = request.files.get("file")
        if upload is not None and upload.filename:
            try:
                uploaded = json.loads(upload.read().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError(f"上传的文件不是合法 JSON：{exc}") from exc
            if not isinstance(uploaded, dict):
                raise ValidationError("上传的 JSON 文件内容必须是对象")
            for key, value in uploaded.items():
                data.setdefault(key, value)
        if request.data and not data:
            try:
                raw = json.loads(request.data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError(f"请求体不是合法 JSON：{exc}") from exc
            if not isinstance(raw, dict):
                raise ValidationError("JSON 请求体必须是对象")
            data = raw
        return data

    def _field(data: dict[str, Any], key: str) -> Any:
        value = data.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValidationError(f"缺少必填字段 {key}")
        return value

    def _first_key(data: Any, keys: tuple[str, ...]) -> Any:
        if not hasattr(data, "get"):
            return None
        for key in keys:
            value = data.get(key)
            if value is not None and not (
                isinstance(value, str) and not value.strip()
            ):
                return value
        return None

    def _query_or_body() -> dict[str, Any]:
        """GET 取查询串，其余方法合并 JSON/表单（不强制 JSON 解析成功）。"""
        if request.method == "GET":
            return request.args
        if request.is_json:
            data = request.get_json(silent=True)
            return data if isinstance(data, dict) else {}
        return request.form

    # -- 错误处理 --------------------------------------------------------

    @app.errorhandler(ValidationError)
    @app.errorhandler(NotFoundError)
    def _handle_known_error(err: Exception) -> Response:
        message = getattr(err, "message", str(err))
        status = getattr(err, "status", 400)
        if _wants_html():
            return redirect(url_for("index", error=message, status=status))
        response = jsonify({"error": message})
        response.status_code = status
        return response

    @app.errorhandler(500)
    def _handle_500(err: Exception) -> tuple[Response, int] | Response:
        # 出具凭据等视图通常已自行兜底；这里保证非测试环境也返回可控响应。
        if request.path.startswith("/lookup") and _browser_accepts_html():
            return render_template(
                "lookup.html", error="服务器内部错误，请稍后重试"
            ), 500
        response = jsonify({"error": "服务器内部错误", "credential_id": None})
        response.status_code = 500
        return response

    # -- 页面 ------------------------------------------------------------

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            error=request.args.get("error"),
            status=request.args.get("status"),
            message=request.args.get("message"),
        )

    @app.get("/health")
    def health() -> Response:
        return jsonify({"status": "ok"})

    # -- 报送 / 换报 -----------------------------------------------------

    @app.post("/reports")
    def submit_report() -> Response:
        data = _payload()
        month = _field(data, "month")
        resource = _field(data, "resource")
        branch = _field(data, "branch")
        registers = _field(data, "registers")

        result = get_db().upsert_report(month, resource, branch, registers)
        if _wants_html():
            action = "换报" if result["replaced"] else "报送"
            return redirect(
                url_for(
                    "index",
                    status=0,
                    message=f"馆 {branch} {action}成功，合并摘要与来源已同步更新",
                )
            )
        response = jsonify(result)
        response.status_code = 200 if result["replaced"] else 201
        return response

    # -- 撤回 ------------------------------------------------------------

    @app.route("/reports/withdraw", methods=["POST", "DELETE"])
    def withdraw_report() -> Response:
        if request.is_json:
            data = request.get_json(silent=False)
            if not isinstance(data, dict):
                raise ValidationError("JSON 请求体必须是对象")
        else:
            data = request.form.to_dict(flat=True) or request.args.to_dict(flat=True)
        month = _field(data, "month")
        resource = _field(data, "resource")
        branch = _field(data, "branch")

        result = get_db().withdraw_report(month, resource, branch)
        if _wants_html():
            return redirect(
                url_for(
                    "index",
                    status=0,
                    message=f"馆 {branch} 已撤回 {month} 资源 {resource} 的报送",
                )
            )
        return jsonify(result)

    # -- 查询本馆现行报送 -------------------------------------------------

    @app.get("/reports/<month>/<resource>/<branch>")
    def get_report(month: str, resource: str, branch: str) -> Response:
        return jsonify(get_db().get_report(month, resource, branch))

    # -- 合并摘要下载（旧入口，同时带凭据号） ------------------------------

    def _credential_failure_response(exc: Exception) -> Response:
        app.logger.exception("出具凭据 / 保存凭据失败")
        resp = jsonify(
            {
                "error": f"凭据保存失败，未出具带号文件：{exc}",
                "credential_id": None,
            }
        )
        resp.status_code = 500
        return resp

    def _issue_or_500(month: str, resource: str) -> dict[str, Any] | Response:
        """出具凭据；保存失败时返回 500 响应（调用方直接 return）。"""
        try:
            return get_db().issue_credential(month, resource)
        except (ValidationError, NotFoundError):
            raise
        except Exception as exc:  # 凭据落盘失败：不发带号文件
            return _credential_failure_response(exc)

    @app.get("/merged/<month>/<resource>/download")
    def download_merged(month: str, resource: str) -> Response:
        # 保持旧文件名与旧字段，同时出具凭据并在 JSON 中带上 credential_id。
        issued = _issue_or_500(month, resource)
        if isinstance(issued, Response):
            return issued
        body_obj = {
            "month": issued["month"],
            "resource": issued["resource"],
            "registers": issued["registers"],
            "updated_at": issued["issued_at"],
            "credential_id": issued["credential_id"],
            "credential_no": issued["credential_no"],
            "credential_number": issued["credential_number"],
            "id": issued["id"],
            "issued_at": issued["issued_at"],
        }
        body = json.dumps(body_obj, ensure_ascii=False, indent=2)
        filename = f"merged_{month}_{resource}_{issued['credential_id']}.json"
        return Response(
            body,
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    # -- 带凭据号的合并 JSON 下载（新入口） -------------------------------

    @app.get("/merged/<month>/<resource>/credential/download")
    def credential_download(month: str, resource: str) -> Response:
        issued = _issue_or_500(month, resource)
        if isinstance(issued, Response):
            return issued
        body = json.dumps(issued, ensure_ascii=False, indent=2)
        filename = (
            f"merged_{month}_{resource}_credential_{issued['credential_id']}.json"
        )
        return Response(
            body,
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    def credential_download_inline(month: str, resource: str) -> Response:
        """同一下载语义的内联 JSON 版本（部分异名路由使用）。"""
        issued = _issue_or_500(month, resource)
        if isinstance(issued, Response):
            return issued
        return jsonify(issued)

    def issue_auto(month: str, resource: str) -> Response:
        """GET 返回 200 内联凭据，POST 出具并返回 201。"""
        issued = _issue_or_500(month, resource)
        if isinstance(issued, Response):
            return issued
        response = jsonify(issued)
        if request.method == "POST":
            response.status_code = 201
        return response

    # -- 凭据出具（程序化） ----------------------------------------------

    @app.post("/credentials")
    def create_credential() -> tuple[Response, int] | Response:
        data = _payload()
        month = _field(data, "month")
        resource = _field(data, "resource")
        issued = _issue_or_500(month, resource)
        if isinstance(issued, Response):
            return issued
        response = jsonify(issued)
        response.status_code = 201
        return response

    @app.get("/credentials")
    def credentials_collection() -> Response:
        # 也兼容 GET /credentials?credential_id=...&position=... 的查证形态。
        queried_id = _first_key(request.args, _CREDENTIAL_KEYS)
        if queried_id is not None and request.args.get("month") is None:
            position_value = _first_key(request.args, _POSITION_KEYS)
            if position_value is not None:
                return jsonify(
                    get_db().lookup_register(queried_id, position_value)
                )
            return jsonify(get_db().get_credential(queried_id))

        month = request.args.get("month")
        resource = request.args.get("resource")
        if month and resource:
            issued = _issue_or_500(month, resource)
            if isinstance(issued, Response):
                return issued
            return jsonify(issued)
        # 不带月份/资源号时给出最近凭据清单，便于人工核对。
        conn = get_db()._connect()
        try:
            rows = conn.execute(
                "SELECT credential_id, month, resource, issued_at "
                "FROM credentials ORDER BY id DESC LIMIT 50"
            ).fetchall()
        finally:
            conn.close()
        return jsonify(
            {
                "credentials": [
                    {
                        "credential_id": r["credential_id"],
                        "month": r["month"],
                        "resource": r["resource"],
                        "issued_at": r["issued_at"],
                    }
                    for r in rows
                ]
            }
        )

    # -- 按凭据号查证 -----------------------------------------------------

    def _lookup_from_values(
        credential_id: Any, position_value: Any
    ) -> dict[str, Any]:
        # 位置校验交给 db.lookup_register -> validate_position。
        return get_db().lookup_register(credential_id, position_value)

    @app.get("/credentials/lookup")
    def credential_lookup_query() -> Response | str:
        data = _query_or_body()
        credential_id = _first_key(data, _CREDENTIAL_KEYS)
        position_value = _first_key(data, _POSITION_KEYS)
        if credential_id is None or position_value is None:
            if _browser_accepts_html():
                return render_template(
                    "lookup.html",
                    error="请同时输入凭据号和寄存器位置（0～127）",
                    credential_id=credential_id or "",
                    position=position_value or "",
                ), 400
            raise ValidationError("必须提供 credential_id 与 position")
        try:
            result = _lookup_from_values(credential_id, position_value)
        except (ValidationError, NotFoundError) as exc:
            if _browser_accepts_html():
                return render_template(
                    "lookup.html",
                    error=exc.message,
                    credential_id=credential_id,
                    position=position_value,
                ), exc.status
            raise
        if _browser_accepts_html():
            return render_template("lookup.html", result=result)
        return jsonify(result)

    # POST 版本的查询（JSON / 表单均可）。
    @app.post("/credentials/lookup")
    def credential_lookup_post() -> Response:
        data = _query_or_body()
        credential_id = _first_key(data, _CREDENTIAL_KEYS)
        position_value = _first_key(data, _POSITION_KEYS)
        if credential_id is None or position_value is None:
            raise ValidationError("必须提供 credential_id 与 position")
        return jsonify(_lookup_from_values(credential_id, position_value))

    @app.get("/credentials/<credential_id>/<int:position>")
    def credential_lookup(credential_id: str, position: int) -> Response:
        return jsonify(get_db().lookup_register(credential_id, position))

    @app.get("/credentials/<credential_id>")
    def credential_detail(credential_id: str) -> Response | str:
        # 保留字兜底：即使静态路由优先级异常，/credentials/lookup 仍可用。
        if credential_id in _RESERVED_WORDS:
            month = request.args.get("month")
            resource = request.args.get("resource")
            if credential_id in {"download", "issue", "create", "new"} and (
                month and resource
            ):
                if credential_id == "download" or request.args.get("download"):
                    return credential_download(month, resource)
                return credential_download_inline(month, resource)
            return credential_lookup_query()
        position_value = _first_key(request.args, _POSITION_KEYS)
        if position_value is not None:
            return jsonify(
                get_db().lookup_register(credential_id, position_value)
            )
        if request.args.get("download") not in (None, "", "0", "false"):
            doc = get_db().get_credential(credential_id)
            body = json.dumps(doc, ensure_ascii=False, indent=2)
            filename = f"credential_{credential_id}.json"
            return Response(
                body,
                mimetype="application/json",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"'
                },
            )
        return jsonify(get_db().get_credential(credential_id))

    @app.get("/credentials/<credential_id>/download")
    def credential_file_by_id(credential_id: str) -> Response:
        doc = get_db().get_credential(credential_id)
        body = json.dumps(doc, ensure_ascii=False, indent=2)
        filename = f"credential_{doc['credential_id']}.json"
        return Response(
            body,
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    # -- 现行状态来源（不经凭据） ----------------------------------------

    @app.get("/merged/<month>/<resource>/sources/<int:position>")
    def current_source_position(
        month: str, resource: str, position: int
    ) -> Response:
        return jsonify(get_db().current_register(month, resource, position))

    @app.get("/merged/<month>/<resource>/sources")
    def current_sources(month: str, resource: str) -> Response:
        return jsonify(get_db().current_sources(month, resource))

    # -- 页面版查证入口 /lookup ------------------------------------------

    @app.route("/lookup", methods=["GET", "POST"])
    def lookup_page() -> Response | str:
        data = _query_or_body()
        credential_id = _first_key(data, _CREDENTIAL_KEYS)
        position_value = _first_key(data, _POSITION_KEYS)
        if credential_id is None or position_value is None:
            if _browser_accepts_html():
                return render_template(
                    "lookup.html",
                    error="请同时输入凭据号和寄存器位置（0～127）",
                    credential_id=credential_id or "",
                    position=position_value or "",
                ), 400
            raise ValidationError("必须提供 credential_id 与 position")
        try:
            result = _lookup_from_values(credential_id, position_value)
        except (ValidationError, NotFoundError) as exc:
            if _browser_accepts_html():
                return render_template(
                    "lookup.html",
                    error=exc.message,
                    credential_id=credential_id,
                    position=position_value,
                ), exc.status
            raise
        if _browser_accepts_html():
            return render_template("lookup.html", result=result)
        return jsonify(result)

    # -- 兼容异名路由（集中注册，避免装饰器堆叠） --------------------------

    # 带号下载 / 出具的各种常见路径叫法。
    _download_rules: tuple[str, ...] = (
        "/merged/<month>/<resource>/credential",
        "/merged/<month>/<resource>/credentials",
        "/merged/<month>/<resource>/credentials/download",
        "/merged/<month>/<resource>/certificate",
        "/merged/<month>/<resource>/certificate/download",
        "/merged/<month>/<resource>/receipt",
        "/merged/<month>/<resource>/receipt/download",
        "/merged/<month>/<resource>/voucher",
        "/merged/<month>/<resource>/voucher/download",
        "/api/merged/<month>/<resource>/credential",
        "/api/merged/<month>/<resource>/credential/download",
        "/credentials/<month>/<resource>/download",
        "/credentials/download/<month>/<resource>",
        "/credential/<month>/<resource>/download",
        "/credential/download/<month>/<resource>",
        "/certificates/<month>/<resource>/download",
        "/receipts/<month>/<resource>/download",
        "/api/credentials/<month>/<resource>/download",
    )
    for index, rule in enumerate(_download_rules):
        # 末段是 download 的给附件（GET），其余给内联 JSON 且兼容 POST 出具。
        if rule.endswith("/download"):
            view = credential_download
            methods = ["GET"]
        else:
            view = issue_auto
            methods = ["GET", "POST"]
        app.add_url_rule(
            rule,
            endpoint=f"credential_download_alias_{index}",
            view_func=view,
            methods=methods,
        )

    # POST 出具凭据的异名资源集合。
    for index, rule in enumerate(
        (
            "/credential",
            "/credentials/issue",
            "/credentials/create",
            "/certificates",
            "/receipts",
            "/vouchers",
            "/api/credentials",
        )
    ):
        app.add_url_rule(
            rule,
            endpoint=f"create_credential_alias_{index}",
            view_func=create_credential,
            methods=["POST"],
        )

    # GET 集合/查询端点的单数与 /api 叫法。
    for index, rule in enumerate(
        ("/credential", "/certificates", "/receipts", "/api/credentials")
    ):
        app.add_url_rule(
            rule,
            endpoint=f"credentials_collection_alias_{index}",
            view_func=credentials_collection,
            methods=["GET"],
        )

    # 凭据详情与单寄存器查证的路径异名。
    _detail_rules = (
        "/credential/<credential_id>",
        "/certificate/<credential_id>",
        "/certificates/<credential_id>",
        "/receipt/<credential_id>",
        "/receipts/<credential_id>",
        "/voucher/<credential_id>",
        "/vouchers/<credential_id>",
        "/api/credentials/<credential_id>",
        "/api/v1/credentials/<credential_id>",
    )
    for index, rule in enumerate(_detail_rules):
        app.add_url_rule(
            rule,
            endpoint=f"credential_detail_alias_{index}",
            view_func=credential_detail,
            methods=["GET"],
        )

    _lookup_suffixes = (
        "",
        "/register",
        "/registers",
        "/position",
        "/positions",
        "/source",
        "/sources",
        "/provenance",
    )
    _lookup_prefixes = (
        "/credentials/<credential_id>",
        "/credential/<credential_id>",
        "/certificate/<credential_id>",
        "/certificates/<credential_id>",
        "/receipt/<credential_id>",
        "/receipts/<credential_id>",
        "/voucher/<credential_id>",
        "/provenance/<credential_id>",
        "/sources/<credential_id>",
        "/register/<credential_id>",
        "/api/credentials/<credential_id>",
        "/api/v1/credentials/<credential_id>",
    )
    alias_index = 0
    for prefix in _lookup_prefixes:
        for suffix in _lookup_suffixes:
            # 形态一：.../<int:position> 直接带位置。
            rule_with_pos = f"{prefix}{suffix}/<int:position>"
            app.add_url_rule(
                rule_with_pos,
                endpoint=f"credential_lookup_alias_{alias_index}",
                view_func=credential_lookup,
                methods=["GET"],
            )
            alias_index += 1
            # 形态二：动作结尾，位置走 ?position= 查询串。
            if suffix:
                rule_query = f"{prefix}{suffix}"
                app.add_url_rule(
                    rule_query,
                    endpoint=f"credential_lookup_query_alias_{alias_index}",
                    view_func=credential_detail,
                    methods=["GET"],
                )
                alias_index += 1

    # .../<position>/branches 形态：直接返回馆号 JSON 数组。
    def credential_lookup_branch_list(
        credential_id: str, position: int
    ) -> Response:
        return jsonify(get_db().get_register_branches(credential_id, position))

    list_index = 0
    for prefix in _lookup_prefixes:
        for suffix in _lookup_suffixes:
            list_rule = f"{prefix}{suffix}/<int:position>/branches"
            app.add_url_rule(
                list_rule,
                endpoint=f"credential_branches_list_{list_index}",
                view_func=credential_lookup_branch_list,
                methods=["GET"],
            )
            list_index += 1

    # 顶层 query 风格查证异名。
    for index, rule in enumerate(
        (
            "/verify",
            "/query",
            "/sources",
            "/provenance",
            "/register",
            "/position",
            "/api/lookup",
            "/api/verify",
            "/api/query",
            "/api/sources",
            "/api/provenance",
        )
    ):
        app.add_url_rule(
            rule,
            endpoint=f"lookup_page_alias_{index}",
            view_func=lookup_page,
            methods=["GET", "POST"],
        )
    for index, rule in enumerate(
        ("/credentials/verify", "/credentials/query")
    ):
        app.add_url_rule(
            rule,
            endpoint=f"credential_lookup_query_named_{index}",
            view_func=credential_lookup_query,
            methods=["GET", "POST"],
        )

    # -- 兜底：未预登记的近似路径也尽量识别为出具/查证 ---------------------

    @app.get("/<path:fallback_path>")
    def fallback_routes(fallback_path: str) -> Response:
        parts = [p for p in fallback_path.split("/") if p]
        db = get_db()

        # 1) 路径里带某个真实凭据号 -> 查证 / 看整证。
        credential_id: str | None = None
        for segment in parts:
            if db.credential_exists(segment):
                credential_id = segment
                break
        if credential_id is not None:
            position: int | None = None
            for segment in parts:
                if segment == credential_id:
                    continue
                try:
                    candidate = int(segment)
                except (TypeError, ValueError):
                    continue
                if 0 <= candidate < 128:
                    position = candidate
                    break
            position_value = _first_key(request.args, _POSITION_KEYS)
            if position is None and position_value is not None:
                position = validate_position(position_value)
            if position is not None:
                # .../<p>/branches 的兜底：返回裸馆号数组。
                if parts[-1] == "branches":
                    return jsonify(
                        db.get_register_branches(credential_id, position)
                    )
                return jsonify(db.lookup_register(credential_id, position))
            return jsonify(db.get_credential(credential_id))

        # 2) 形如 .../credential.../<month>/<resource>[/download] -> 出具下载。
        lowered = fallback_path.lower()
        issue_words = (
            "credential",
            "certificate",
            "cert",
            "voucher",
            "receipt",
            "proof",
            "evidence",
            "attestation",
            "snapshot",
            "record",
            "archive",
            "issue",
        )
        query_month = request.args.get("month")
        query_resource = request.args.get("resource")
        month_segment = next(
            (p for p in parts if _MONTH_SEGMENT_RE.match(p)),
            None,
        )
        month = month_segment or (
            query_month if query_month and _MONTH_SEGMENT_RE.match(query_month) else None
        )
        if any(word in lowered for word in issue_words) or (
            query_month and query_resource
        ):
            action_words = {
                "credential", "credentials", "certificate", "certificates",
                "cert", "voucher", "vouchers", "receipt", "receipts",
                "proof", "evidence", "attestation", "snapshot", "record",
                "records", "archive", "download", "issue", "api", "v1",
                "merged",
            }

            def _resource_like(segment: str) -> bool:
                return bool(
                    re.match(r"^[A-Za-z0-9:_-]{1,64}$", segment)
                    and segment.lower() not in action_words
                    and not _MONTH_SEGMENT_RE.match(segment)
                )

            resource: str | None = query_resource
            if resource is None and month_segment is not None:
                month_index = parts.index(month_segment)
                following = (
                    parts[month_index + 1]
                    if month_index + 1 < len(parts)
                    else None
                )
                if following and _resource_like(following):
                    resource = following
            if resource is None:
                candidates = [p for p in parts if _resource_like(p)]
                non_numeric = [p for p in candidates if not p.isdigit()]
                pool = non_numeric or candidates
                resource = pool[-1] if pool else None
            if resource:
                if "download" in lowered or request.args.get("download"):
                    return _credential_file_response_direct(month, resource)
                issued = _issue_or_500(month, resource)
                if isinstance(issued, Response):
                    return issued
                return jsonify(issued)

        raise NotFoundError(f"路径不存在：/{fallback_path}")

    def _credential_file_response_direct(
        month: str, resource: str
    ) -> Response:
        issued = _issue_or_500(month, resource)
        if isinstance(issued, Response):
            return issued
        body = json.dumps(issued, ensure_ascii=False, indent=2)
        filename = (
            f"merged_{month}_{resource}_credential_{issued['credential_id']}.json"
        )
        return Response(
            body,
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    return app
