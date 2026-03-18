"""Plan endpoints: select, reject, detail."""

from fastapi import APIRouter

from app.models.schemas import PlanDetail, PlanSelectRequest
from app.services.analytics import analytics
from app.services.chat_service import ChatService

router = APIRouter()

# Import the shared chat_service to access plan_service with stored plans
from app.api.chat import chat_service


@router.post("/select")
async def select_plan(req: PlanSelectRequest) -> dict[str, object]:
    """User selects a plan."""
    session_id = req.session_id or ""

    # Store selected plan in session state
    if session_id:
        session = await chat_service._get_session(session_id)
        session["selected_plan"] = req.plan_id
        await chat_service._save_session(session_id, session)

    # Get plan details for confirmation
    detail = await chat_service.plan_service.get_plan_detail(req.plan_id)

    await analytics.track("plan_selected", session_id=session_id,
                          properties={"plan_id": req.plan_id})

    return {
        "status": "ok",
        "plan_id": req.plan_id,
        "message": "方案已选择",
        "plan_title": detail.title,
        "stops_count": len(detail.stops),
    }


@router.post("/reject")
async def reject_plan(req: PlanSelectRequest) -> dict[str, object]:
    """User rejects current plans, requesting regeneration."""
    session_id = req.session_id or ""

    rejection_count = 0
    if session_id:
        session = await chat_service._get_session(session_id)
        # Track rejected plan titles
        for plan in session.get("current_plans", []):
            title = plan.title if hasattr(plan, "title") else plan.get("title", "")
            if title:
                session["rejected_plans"].append(title)
        session["rejection_count"] = session.get("rejection_count", 0) + 1
        rejection_count = session["rejection_count"]
        await chat_service._save_session(session_id, session)

    await analytics.track("plan_rejected", session_id=session_id,
                          properties={"plan_id": req.plan_id, "rejection_count": rejection_count})

    return {"status": "ok", "message": "已记录，将为您重新生成方案"}


@router.get("/detail/{plan_id}")
async def get_plan_detail(plan_id: str) -> PlanDetail:
    """Get full plan detail with stops, tips, sources."""
    await analytics.track("plan_detail_viewed", properties={"plan_id": plan_id})
    return await chat_service.plan_service.get_plan_detail(plan_id)
