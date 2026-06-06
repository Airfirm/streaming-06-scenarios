"""src/streaming/visualizations/live_visualizations_femi.py.

Project-specific live visualization functions used by the Kafka consumer.

Technical Modification:
- Updates the chart title and labels for the custom monitoring problem.
- Shows running sales totals as messages are consumed.

Author: O S
Date: 2026-06-06
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

# === DECLARE EXPORTS ===

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


def init_live_chart() -> tuple[Any, Any, list[int], list[float]]:
    """Create and show an empty live chart."""
    plt.ion()

    figure, axis = plt.subplots()

    x_values: list[int] = []
    y_values: list[float] = []

    axis.set_title("Real-Time Sales Total by Kafka Message")
    axis.set_xlabel("Kafka Message Offset")
    axis.set_ylabel("Sale Total ($)")
    axis.grid(True)

    figure.show()
    figure.canvas.draw()
    figure.canvas.flush_events()

    return figure, axis, x_values, y_values


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
    message: dict[str, Any],
) -> None:
    """Update the live chart with one consumed message."""
    new_x = int(message["_kafka_offset"])
    x_values.append(new_x)

    new_y = float(message["total"])
    y_values.append(new_y)

    axis.clear()
    axis.plot(x_values, y_values, marker="o")

    axis.set_title("Real-Time Sales Total by Kafka Message")
    axis.set_xlabel("Kafka Message Offset")
    axis.set_ylabel("Sale Total ($)")
    axis.grid(True)

    figure.canvas.draw()
    figure.canvas.flush_events()

    plt.pause(0.05)


def save_live_chart(
    *,
    figure: Any,
    chart_path: Path,
) -> None:
    """Save the final live chart to an image file."""
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart_path, bbox_inches="tight")


def close_live_chart() -> None:
    """Turn off interactive chart mode."""
    plt.ioff()
