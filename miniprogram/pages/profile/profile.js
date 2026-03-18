// pages/profile/profile.js — 个人中心
const api = require('../../utils/api');

Page({
  data: {
    historyPlans: [],
    loading: false,
    isEmpty: true,
  },

  onLoad() {
    this.loadHistory();
  },

  onShow() {
    this.loadHistory();
  },

  async loadHistory() {
    const app = getApp();
    const sessionId = app.globalData.sessionId;

    if (!sessionId) {
      this.setData({ historyPlans: [], isEmpty: true, loading: false });
      return;
    }

    this.setData({ loading: true });

    try {
      const res = await api.getHistory(sessionId);
      const plans = (res.plans || []).map(plan => ({
        ...plan,
        dateText: plan.created_at ? new Date(plan.created_at).toLocaleDateString('zh-CN') : '',
      }));
      this.setData({
        historyPlans: plans,
        isEmpty: plans.length === 0,
        loading: false,
      });
    } catch (err) {
      console.error('Failed to load history:', err);
      this.setData({ historyPlans: [], isEmpty: true, loading: false });
    }
  },

  onPlanTap(e) {
    const planId = e.currentTarget.dataset.planId;
    if (planId) {
      wx.navigateTo({
        url: '/pages/plan-detail/plan-detail?id=' + planId,
      });
    }
  },
});
