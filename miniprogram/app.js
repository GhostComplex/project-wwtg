// app.js — 周末搭子小程序入口
App({
  onLaunch() {
    console.log('周末搭子启动');
    this._initEnvConfig();
  },

  _initEnvConfig() {
    // Detect environment via wx.getAccountInfoSync (available in base library 2.2.2+)
    let envType = 'develop'; // default to dev
    try {
      const accountInfo = wx.getAccountInfoSync();
      envType = accountInfo.miniProgram.envVersion || 'develop';
    } catch (e) {
      console.warn('getAccountInfoSync not available, defaulting to develop');
    }

    if (envType === 'release') {
      // Production
      this.globalData.baseURL = 'https://api.wwtg.example.com/api/v1';
      this.globalData.mockMode = false;
    } else if (envType === 'trial') {
      // Staging / trial
      this.globalData.baseURL = 'https://staging-api.wwtg.example.com/api/v1';
      this.globalData.mockMode = false;
    } else {
      // develop / unknown → local dev
      this.globalData.baseURL = 'http://localhost:8000/api/v1';
      this.globalData.mockMode = false;
    }

    console.log(`Environment: ${envType}, baseURL: ${this.globalData.baseURL}, mockMode: ${this.globalData.mockMode}`);
  },

  globalData: {
    baseURL: 'http://localhost:8000/api/v1',
    mockMode: true,
    userInfo: null,
    sessionId: null,
  },
});
