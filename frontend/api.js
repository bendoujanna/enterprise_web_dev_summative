const API_BASE = 'http://127.0.0.1:5000/api';

const API = {
    async call(endpoint) {
        try {
            const response = await fetch(`${API_BASE}${endpoint}`);
            return await response.json();
        } catch (err) {
            console.error("API Error:", err);
            return null;
        }
    },
    getSummary: () => API.call('/stats/summary'),
    getQuality: () => API.call('/stats/quality'),
    getBoroughDist: () => API.call('/stats/charts/boroughs'),
    getSpeedEff: () => API.call('/stats/charts/efficiency'),
    getAnalytics: () => API.call('/analytics/summary'),
    getCustomRevenue: () => API.call('/analytics/borough-custom'),
    getTopExpensive: (n = 10) => API.call(`/trips/top-expensive?n=${n}`),
    getSortedTrips: (sortBy = 'total_amount', limit = 10, borough = '') => {
        let url = `/trips/custom-sort?sort_by=${sortBy}&limit=${limit}`;
        if (borough) url += `&borough=${encodeURIComponent(borough)}`;
        return API.call(url);
    }
};

