window.SAMPLE_ALERTS = {
  summary: {
    city: "New Delhi",
    total_alerts: 4,
    critical: 1,
    high: 2,
    medium: 1,
    low: 0
  },
  alerts: [
    {
      city: "New Delhi",
      alert_id: "demo-critical-001",
      track_id: "track-91a",
      observed_at: "2026-05-30T10:32:00Z",
      severity: "critical",
      score: 91.2,
      lat: 28.6139,
      lon: 77.209,
      sensor_type: "vehicle_iot",
      reason_codes: ["high_speed", "near_restricted_zone", "multi_sensor_corroboration"],
      status: "needs_human_review"
    },
    {
      city: "New Delhi",
      alert_id: "demo-high-002",
      track_id: "track-5bc",
      observed_at: "2026-05-30T10:35:00Z",
      severity: "high",
      score: 78.5,
      lat: 28.63,
      lon: 77.22,
      sensor_type: "radar",
      reason_codes: ["route_deviation", "multi_sensor_corroboration"],
      status: "needs_human_review"
    },
    {
      city: "New Delhi",
      alert_id: "demo-high-003",
      track_id: "track-d2e",
      observed_at: "2026-05-30T10:37:00Z",
      severity: "high",
      score: 73.8,
      lat: 28.59,
      lon: 77.18,
      sensor_type: "aircraft_iot",
      reason_codes: ["route_deviation", "metadata_only_signal"],
      status: "needs_human_review"
    },
    {
      city: "New Delhi",
      alert_id: "demo-medium-004",
      track_id: "track-1ff",
      observed_at: "2026-05-30T10:40:00Z",
      severity: "medium",
      score: 55.1,
      lat: 28.65,
      lon: 77.24,
      sensor_type: "sigint_metadata",
      reason_codes: ["metadata_only_signal", "identifier_obscured"],
      status: "needs_human_review"
    }
  ]
};
