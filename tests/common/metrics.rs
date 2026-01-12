//! Test helpers for collecting and comparing Prometheus metrics
//!
//! Provides snapshot functionality to capture metrics before/after changes
//! for performance regression testing.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use prometheus::Registry;
use serde::{Deserialize, Serialize};

/// Snapshot of Prometheus metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp: i64,
    pub git_commit: Option<String>,
    pub test_name: String,
    pub metrics: HashMap<String, MetricValue>,
}

/// Metric value with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricValue {
    pub value: f64,
    pub metric_type: String,
    pub help: String,
}

/// Comparison result between two metrics snapshots
#[derive(Debug)]
pub struct MetricsComparison {
    pub baseline: MetricsSnapshot,
    pub current: MetricsSnapshot,
    pub improvements: Vec<MetricDelta>,
    pub regressions: Vec<MetricDelta>,
    pub unchanged: Vec<String>,
}

/// Delta between baseline and current metric
#[derive(Debug, Clone)]
pub struct MetricDelta {
    pub name: String,
    pub baseline_value: f64,
    pub current_value: f64,
    pub delta: f64,
    pub percent_change: f64,
}

impl MetricsSnapshot {
    /// Capture current metrics from a Prometheus registry
    pub fn capture(registry: &Registry, test_name: &str) -> Self {
        use prometheus::Encoder;

        let mut metrics = HashMap::new();

        // Gather metrics and encode to text format
        let encoder = prometheus::TextEncoder::new();
        let metric_families = registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        // Parse text format to extract values
        let text = String::from_utf8(buffer).unwrap();

        for line in text.lines() {
            // Skip comments and empty lines
            if line.starts_with('#') || line.is_empty() {
                continue;
            }

            // Parse metric line: "metric_name{labels} value"
            if let Some((name_part, value_str)) = line.rsplit_once(' ') {
                // Extract metric name (before { or space)
                let name = if let Some(idx) = name_part.find('{') {
                    &name_part[..idx]
                } else {
                    name_part
                };

                if let Ok(value) = value_str.parse::<f64>() {
                    // Aggregate values for same metric name
                    metrics.entry(name.to_string())
                        .and_modify(|m: &mut MetricValue| m.value += value)
                        .or_insert(MetricValue {
                            value,
                            metric_type: "Unknown".to_string(),
                            help: String::new(),
                        });
                }
            }
        }

        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            git_commit: get_git_commit(),
            test_name: test_name.to_string(),
            metrics,
        }
    }

    /// Save snapshot to JSON file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), String> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize: {}", e))?;

        fs::write(path, json)
            .map_err(|e| format!("Failed to write file: {}", e))?;

        Ok(())
    }

    /// Load snapshot from JSON file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let json = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read file: {}", e))?;

        serde_json::from_str(&json)
            .map_err(|e| format!("Failed to deserialize: {}", e))
    }

    /// Compare this snapshot with another (self is current, other is baseline)
    pub fn compare(&self, baseline: &MetricsSnapshot) -> MetricsComparison {
        let mut improvements = Vec::new();
        let mut regressions = Vec::new();
        let mut unchanged = Vec::new();

        // Compare metrics present in both snapshots
        for (name, current_metric) in &self.metrics {
            if let Some(baseline_metric) = baseline.metrics.get(name) {
                let delta = current_metric.value - baseline_metric.value;
                let percent_change = if baseline_metric.value != 0.0 {
                    (delta / baseline_metric.value) * 100.0
                } else if delta != 0.0 {
                    100.0 // 0 -> non-zero is 100% change
                } else {
                    0.0
                };

                // Determine if this is an improvement or regression
                // For latency/errors: lower is better
                // For throughput/processed: higher is better
                let is_improvement = is_metric_improvement(name, delta);

                if delta.abs() < 0.001 {
                    // No significant change
                    unchanged.push(name.clone());
                } else if is_improvement {
                    improvements.push(MetricDelta {
                        name: name.clone(),
                        baseline_value: baseline_metric.value,
                        current_value: current_metric.value,
                        delta,
                        percent_change,
                    });
                } else {
                    regressions.push(MetricDelta {
                        name: name.clone(),
                        baseline_value: baseline_metric.value,
                        current_value: current_metric.value,
                        delta,
                        percent_change,
                    });
                }
            }
        }

        // Sort by absolute percent change
        improvements.sort_by(|a, b| {
            b.percent_change.abs().partial_cmp(&a.percent_change.abs()).unwrap()
        });
        regressions.sort_by(|a, b| {
            b.percent_change.abs().partial_cmp(&a.percent_change.abs()).unwrap()
        });

        MetricsComparison {
            baseline: baseline.clone(),
            current: self.clone(),
            improvements,
            regressions,
            unchanged,
        }
    }
}

impl MetricsComparison {
    /// Print comparison report to stdout
    pub fn print_report(&self) {
        println!("\n=== Metrics Comparison Report ===");
        println!("Baseline: {} ({})",
            self.baseline.test_name,
            self.baseline.git_commit.as_deref().unwrap_or("unknown")
        );
        println!("Current:  {} ({})",
            self.current.test_name,
            self.current.git_commit.as_deref().unwrap_or("unknown")
        );

        if !self.improvements.is_empty() {
            println!("\n✅ Improvements ({}):", self.improvements.len());
            for delta in &self.improvements {
                println!("  {} : {:.2} → {:.2} ({:+.1}%)",
                    delta.name,
                    delta.baseline_value,
                    delta.current_value,
                    delta.percent_change
                );
            }
        }

        if !self.regressions.is_empty() {
            println!("\n❌ Regressions ({}):", self.regressions.len());
            for delta in &self.regressions {
                println!("  {} : {:.2} → {:.2} ({:+.1}%)",
                    delta.name,
                    delta.baseline_value,
                    delta.current_value,
                    delta.percent_change
                );
            }
        }

        println!("\n📊 Summary:");
        println!("  Improvements: {}", self.improvements.len());
        println!("  Regressions:  {}", self.regressions.len());
        println!("  Unchanged:    {}", self.unchanged.len());

        // Overall verdict
        if self.regressions.is_empty() && !self.improvements.is_empty() {
            println!("\n✅ PASS: Performance improved with no regressions");
        } else if !self.regressions.is_empty() {
            println!("\n⚠️  WARNING: Performance regressions detected");
        } else {
            println!("\n➡️  No significant performance changes");
        }
    }

    /// Save comparison report to markdown file
    pub fn save_markdown<P: AsRef<Path>>(&self, path: P) -> Result<(), String> {
        let mut md = String::new();

        md.push_str("# Metrics Comparison Report\n\n");
        md.push_str(&format!("**Baseline:** {} ({})\n\n",
            self.baseline.test_name,
            self.baseline.git_commit.as_deref().unwrap_or("unknown")
        ));
        md.push_str(&format!("**Current:** {} ({})\n\n",
            self.current.test_name,
            self.current.git_commit.as_deref().unwrap_or("unknown")
        ));

        if !self.improvements.is_empty() {
            md.push_str("## ✅ Improvements\n\n");
            md.push_str("| Metric | Baseline | Current | Change | % |\n");
            md.push_str("|--------|----------|---------|--------|---|\n");
            for delta in &self.improvements {
                md.push_str(&format!("| `{}` | {:.2} | {:.2} | {:+.2} | {:+.1}% |\n",
                    delta.name,
                    delta.baseline_value,
                    delta.current_value,
                    delta.delta,
                    delta.percent_change
                ));
            }
            md.push_str("\n");
        }

        if !self.regressions.is_empty() {
            md.push_str("## ❌ Regressions\n\n");
            md.push_str("| Metric | Baseline | Current | Change | % |\n");
            md.push_str("|--------|----------|---------|--------|---|\n");
            for delta in &self.regressions {
                md.push_str(&format!("| `{}` | {:.2} | {:.2} | {:+.2} | {:+.1}% |\n",
                    delta.name,
                    delta.baseline_value,
                    delta.current_value,
                    delta.delta,
                    delta.percent_change
                ));
            }
            md.push_str("\n");
        }

        md.push_str("## Summary\n\n");
        md.push_str(&format!("- **Improvements:** {}\n", self.improvements.len()));
        md.push_str(&format!("- **Regressions:** {}\n", self.regressions.len()));
        md.push_str(&format!("- **Unchanged:** {}\n", self.unchanged.len()));

        fs::write(path, md)
            .map_err(|e| format!("Failed to write markdown: {}", e))
    }
}

/// Determine if a metric change is an improvement
fn is_metric_improvement(metric_name: &str, delta: f64) -> bool {
    // For these metrics, lower is better
    if metric_name.contains("latency")
        || metric_name.contains("error")
        || metric_name.contains("dlq")
        || metric_name.contains("lag")
    {
        return delta < 0.0; // Decrease is improvement
    }

    // For throughput metrics, higher is better
    if metric_name.contains("processed")
        || metric_name.contains("inserted")
        || metric_name.contains("flushed")
    {
        return delta > 0.0; // Increase is improvement
    }

    // For buffer metrics, lower is better (less memory)
    if metric_name.contains("buffer") || metric_name.contains("memory") {
        return delta < 0.0;
    }

    // Default: assume increase is improvement
    delta > 0.0
}

/// Get current git commit hash (if in git repo)
fn get_git_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(&["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Counter, Gauge, Histogram, Opts, Registry};

    #[test]
    fn test_metrics_snapshot_capture() {
        let registry = Registry::new();

        let counter = Counter::with_opts(Opts::new("test_counter", "Test counter")).unwrap();
        counter.inc_by(100.0);
        registry.register(Box::new(counter)).unwrap();

        let snapshot = MetricsSnapshot::capture(&registry, "test");

        assert_eq!(snapshot.test_name, "test");
        assert!(snapshot.metrics.contains_key("test_counter"));
        assert_eq!(snapshot.metrics.get("test_counter").unwrap().value, 100.0);
    }

    #[test]
    fn test_metrics_comparison() {
        let mut baseline_metrics = HashMap::new();
        baseline_metrics.insert(
            "loader_messages_processed_total".to_string(),
            MetricValue {
                value: 1000.0,
                metric_type: "Counter".to_string(),
                help: "Test".to_string(),
            },
        );
        baseline_metrics.insert(
            "loader_insert_latency_seconds".to_string(),
            MetricValue {
                value: 0.5,
                metric_type: "Histogram".to_string(),
                help: "Test".to_string(),
            },
        );

        let mut current_metrics = HashMap::new();
        current_metrics.insert(
            "loader_messages_processed_total".to_string(),
            MetricValue {
                value: 1200.0, // 20% improvement
                metric_type: "Counter".to_string(),
                help: "Test".to_string(),
            },
        );
        current_metrics.insert(
            "loader_insert_latency_seconds".to_string(),
            MetricValue {
                value: 0.4, // 20% improvement (lower latency)
                metric_type: "Histogram".to_string(),
                help: "Test".to_string(),
            },
        );

        let baseline = MetricsSnapshot {
            timestamp: 0,
            git_commit: Some("abc123".to_string()),
            test_name: "baseline".to_string(),
            metrics: baseline_metrics,
        };

        let current = MetricsSnapshot {
            timestamp: 1000,
            git_commit: Some("def456".to_string()),
            test_name: "current".to_string(),
            metrics: current_metrics,
        };

        let comparison = current.compare(&baseline);

        assert_eq!(comparison.improvements.len(), 2);
        assert_eq!(comparison.regressions.len(), 0);

        // Check throughput improvement
        let throughput_delta = comparison.improvements.iter()
            .find(|d| d.name == "loader_messages_processed_total")
            .unwrap();
        assert_eq!(throughput_delta.delta, 200.0);
        assert!((throughput_delta.percent_change - 20.0).abs() < 0.1);

        // Check latency improvement
        let latency_delta = comparison.improvements.iter()
            .find(|d| d.name == "loader_insert_latency_seconds")
            .unwrap();
        assert!((latency_delta.delta + 0.1).abs() < 0.01, "Delta should be approximately -0.1");
        assert!((latency_delta.percent_change + 20.0).abs() < 0.1);
    }
}
