"""Plan endpoints: select, reject, detail."""

from fastapi import APIRouter

from app.models.schemas import PlanDetail, PlanStop, PlanSource, PlanSelectRequest
from app.services.analytics import analytics
from app.services.chat_service import ChatService

router = APIRouter()


# --- Temporary test endpoint for navigation testing (remove after) ---
@router.post("/inject-test-plan")
async def inject_test_plan():
    """Inject a test plan with real nav_links for navigation testing."""
    chat_service = ChatService.get_instance()
    test_plan = {
        "plan_id": "test-nav",
        "title": "导航测试方案 · 苏州经典三站",
        "stops": [
            {
                "name": "拙政园",
                "arrive_at": "10:00",
                "stay_duration": "90分钟",
                "recommendation": "世界文化遗产，苏州园林代表",
                "nav_link": "https://uri.amap.com/marker?position=120.631,31.324&name=%E6%8B%99%E6%94%BF%E5%9B%AD",
                "walk_to_next": "步行约800m，10分钟",
            },
            {
                "name": "平江路",
                "arrive_at": "11:45",
                "stay_duration": "60分钟",
                "recommendation": "古街漫步，小吃咖啡",
                "nav_link": "https://uri.amap.com/marker?position=120.636,31.316&name=%E5%B9%B3%E6%B1%9F%E8%B7%AF",
                "walk_to_next": "步行约500m，6分钟",
            },
            {
                "name": "苏州博物馆",
                "arrive_at": "13:00",
                "stay_duration": "60分钟",
                "recommendation": "贝聿铭设计，免费参观",
                "nav_link": "https://uri.amap.com/marker?position=120.630,31.323&name=%E8%8B%8F%E5%B7%9E%E5%8D%9A%E7%89%A9%E9%A6%86",
                "walk_to_next": "",
            },
        ],
        "tips": ["全程步行约1.3km", "拙政园建议提前预约"],
        "sources": [],
    }
    chat_service.plan_service._plans["test-nav"] = test_plan
    return {"status": "ok", "plan_id": "test-nav"}

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
