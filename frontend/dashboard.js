let charts = {};

document.addEventListener('DOMContentLoaded', () => {
    setupNavigation();
    setupDarkMode();
    loadDashboard();
    setupFilters();
});
// navigation and view switching
function setupNavigation() {
    const links = document.querySelectorAll('.nav-links li');
    links.forEach(link => {
        link.addEventListener('click', () => {
            // Update Active State
            links.forEach(l => l.classList.remove('active'));
            link.classList.add('active');

            // Switch Views
            const viewId = link.getAttribute('data-view');
            document.querySelectorAll('.view').forEach(v => v.style.display = 'none');
            document.getElementById(`${viewId}-view`).style.display = 'block';

            // Load Data for Specific Views
            if (viewId === 'dashboard') loadDashboard();
            if (viewId === 'analytics') loadAnalytics();
            if (viewId === 'quality') loadQuality();
        });
    });
}

// dark mode
function setupDarkMode() {
    const toggle = document.getElementById('dark-mode-toggle');
    if (!toggle) return;

    toggle.addEventListener('click', () => {
        document.body.classList.toggle('dark-mode');
        document.body.classList.toggle('light-mode');
        // Refresh charts to apply new theme colors
        Object.values(charts).forEach(c => c.update());
    });
}

// dashboard
async function loadDashboard() {
    // Fetch all data in parallel for speed
    const [summary, quality, boroughs, speed] = await Promise.all([
        API.getSummary(),
        API.getQuality(),
        API.getBoroughDist(),
        API.getSpeedEff()
    ]);

    // Update KPIs
    if (summary) {
        document.getElementById('total-trips').textContent = summary.total_trips.toLocaleString();
        document.getElementById('avg-fare').textContent = `$${summary.avg_fare}`;
    }
    if (quality) {
        document.getElementById('quality-score').textContent = quality.overall_score;
        document.getElementById('rejected-count-text').textContent = `${quality.rejected_records.toLocaleString()} rejected records`;
    }

    // Render Charts and table
    if (boroughs) renderBoroughChart(boroughs);
    if (speed) renderSpeedChart(speed);
    loadTripsTable(); // Initial load
}

// analytics
async function loadAnalytics() {
    const data = await API.getAnalytics();

    if (data) {
        document.getElementById('total-revenue').textContent = data.kpis.total_revenue;
        document.getElementById('avg-duration').textContent = data.kpis.avg_trip_duration;
        renderHourlyChart(data.chart_data);
    }
}

// loader
async function loadQuality() {
    const data = await API.getQuality();
    if (data) {
        document.getElementById('overall-score-large').textContent = data.overall_score;
        const list = document.getElementById('issue-list');


        list.innerHTML = data.detailed_issues.map(issue => `
            <div class="card quality-item ${issue.status}">
                <div style="display:flex; justify-content:space-between;">
                    <span>${issue.issue}</span>
                    <strong>${issue.count.toLocaleString()}</strong>
                </div>
                <div class="progress-bar">
                    <div style="width: ${Math.min(issue.count / 1000, 100)}%; background: var(--status-${issue.status});"></div>
                </div>
            </div>
        `).join('');
    }
}

// chart renderers
// function renderBoroughChart(data) {
//     const ctx = document.getElementById('boroughChart').getContext('2d');
//     updateChart('borough', ctx, {
//         type: 'bar',
//         data: {
//             labels: data.map(d => d.Borough),
//             datasets: [{
//                 label: 'Trips',
//                 data: data.map(d => d.trip_count),
//                 backgroundColor: '#4a90e2'
//             }]
//         },
//         options: { indexAxis: 'y', responsive: true }
//     });
// }

function renderBoroughChart(data) {
    const ctx = document.getElementById('boroughChart').getContext('2d');
    updateChart('borough', ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.Borough),
            datasets: [{
                label: 'Trips',
                data: data.map(d => d.trip_count),
                backgroundColor: '#4a90e2'
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'logarithmic'
                },
                y: {
                    ticks: {
                        autoSkip: false
                    }
                }
            }
        }
    });
}

function renderSpeedChart(data) {
    const ctx = document.getElementById('speedChart').getContext('2d');
    updateChart('speed', ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.time_of_day),
            datasets: [{
                label: 'Avg Speed (mph)',
                data: data.map(d => d.avg_speed),
                borderColor: '#1e3c72',
                backgroundColor: 'rgba(30, 60, 114, 0.1)',
                tension: 0.4,
                fill: true
            }]
        }
    });
}

function renderHourlyChart(chartData) {
    const ctx = document.getElementById('hourlyChart').getContext('2d');
    updateChart('hourly', ctx, {
        type: 'bar',
        data: {
            labels: chartData.map(d => d.hour),
            datasets: [{
                label: 'Trips per Hour',
                data: chartData.map(d => d.trips),
                backgroundColor: '#1e3c72'
            }]
        }
    });
}

// Helper to destroy old charts before creating new ones
function updateChart(id, ctx, config) {
    if (charts[id]) charts[id].destroy();
    charts[id] = new Chart(ctx, config);
}

// filters and sorting
function setupFilters() {
    const boroughSelect = document.getElementById('borough-filter');
    const sortSelect = document.getElementById('sort-selector');

    const triggerRefresh = () => {
        const borough = boroughSelect.value;
        const sortBy = sortSelect.value;
        // Pass the values to the loader
        loadTripsTable(borough, sortBy);
    };

    boroughSelect.addEventListener('change', triggerRefresh);
    sortSelect.addEventListener('change', triggerRefresh);
}

// main table loader
async function loadTripsTable(borough = '', sortBy = 'total_amount') {
    const tbody = document.getElementById('trips-tbody');
    tbody.innerHTML = '<tr><td colspan="7" class="loading"><i class="fa-solid fa-spinner fa-spin"></i> Processing Manual Sort...</td></tr>';

    //  Call API
    const res = await API.getSortedTrips(sortBy, 100, borough);

    // Render Rows
    if (res && res.data) {
        tbody.innerHTML = res.data.map(trip => `
            <tr>
                <td style="font-weight: bold; color: var(--primary-blue);">
                    ${trip.pickup_borough || 'Unknown'}
                </td>
                <td>Zone ${trip.pickup_location}</td>
                <td>Zone ${trip.dropoff_location}</td>
                <td>$${parseFloat(trip.total_amount).toFixed(2)}</td>
                <td>${trip.trip_distance} mi</td>
                <td>${parseFloat(trip.speed).toFixed(1)} mph</td>
                <td>${trip.pickup_time.split(' ')[1]}</td>
            </tr>
        `).join('');
    } else {
        tbody.innerHTML = '<tr><td colspan="7">No records found.</td></tr>';
    }
}