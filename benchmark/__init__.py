def write_memory_markdown_report(
    path: str,
    report: dict,
    *,
    heading: str,
    progress: str,
    extra_lines=(),
) -> None:
    baseline = report["baseline"]
    final = report["final"]
    rows = (
        ("RSS", baseline["rss_mib"], final["rss_mib"], report["rss_growth_mib"], "MiB"),
        (
            "Python heap",
            baseline["py_current_mib"],
            final["py_current_mib"],
            report["python_heap_growth_mib"],
            "MiB",
        ),
        ("File descriptors", baseline["fd_count"], final["fd_count"], report["fd_growth"], ""),
        (
            "Pending tasks",
            baseline["pending_tasks"],
            final["pending_tasks"],
            report["pending_task_growth"],
            "",
        ),
    )
    lines = [
        f"### {heading}",
        "",
        progress,
        "",
        "| Metric | Baseline | Final | Growth |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, baseline_value, final_value, growth, unit in rows:
        suffix = f" {unit}" if unit else ""
        lines.append(
            f"| {name} | {baseline_value:.2f}{suffix} | {final_value:.2f}{suffix} | "
            f"{growth:+.2f}{suffix} |"
        )
    lines.extend(extra_lines)
    with open(path, "w", encoding="utf-8") as report_file:
        report_file.write("\n".join(lines) + "\n")
