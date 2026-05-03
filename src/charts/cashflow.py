"""Stacked-bar cashflow chart.

Each period's bar has up to 5 stacked segments (one per category). Positive
amounts stack upward from y=0; negative amounts stack downward. A black
net line with markers connects each period's net total. Lumpy payments
(e.g. an annual property tax) appear as a tall colored segment in the
month they actually occurred — the breakdown explains the spike rather
than hiding it.
"""
from typing import Literal

from matplotlib.figure import Figure

from src.engines.cashflow import CashflowPeriod
from src.charts.dashboard import _compact_money_tick, _empty_figure


_TEXT_COLOR = "#cccccc"
_GRID_COLOR = "#444444"
_NET_LINE_COLOR = "#ffffff"
_ZERO_LINE_COLOR = "#888888"

# Category palette — chosen to match the semantic meaning each color carries
# elsewhere in the app: green for income (rent), red for debt drag, blue for
# trade activity, gray for funding (neutral cash movement), purple for "other".
_CATEGORY_COLORS = {
    "funding_flow": "#9e9e9e",
    "trade_cash_flow": "#1565c0",
    "real_estate_cash_flow": "#2e7d32",
    "debt_cash_flow": "#c62828",
    "other_cash_flow": "#6a1b9a",
}
_CATEGORY_LABELS = {
    "funding_flow": "Funding",
    "trade_cash_flow": "Trades",
    "real_estate_cash_flow": "Real Estate",
    "debt_cash_flow": "Debt",
    "other_cash_flow": "Other",
}
_CATEGORIES = (
    "funding_flow", "trade_cash_flow", "real_estate_cash_flow",
    "debt_cash_flow", "other_cash_flow",
)


def _has_any_activity(periods: list[CashflowPeriod]) -> bool:
    return any(
        getattr(p, cat) != 0 for p in periods for cat in _CATEGORIES
    )


def create_cashflow_bar_figure(
    periods: list[CashflowPeriod],
    granularity: Literal["monthly", "yearly"] = "monthly",
    compact: bool = False,
) -> Figure:
    """Stacked bar chart of cashflow per period with a net line overlay.

    Empty periods stay in the x-axis to keep it contiguous. If every
    period is zero, returns an empty-state figure.
    """
    if not periods:
        return _empty_figure("No cashflow data")
    if not _has_any_activity(periods):
        return _empty_figure("No transactions in selected range")

    fig = Figure(figsize=(7, 3.2), dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    ax.set_facecolor("none")
    ax.tick_params(colors=_TEXT_COLOR, labelsize=7 if compact else 8)
    for spine in ax.spines.values():
        spine.set_color(_GRID_COLOR)
    ax.yaxis.set_major_formatter(_compact_money_tick)

    n = len(periods)
    x = list(range(n))
    bar_width = 0.7

    # For each bar we track running totals separately for positive and
    # negative segments so the same category doesn't visually cross zero.
    positive_running = [0.0] * n
    negative_running = [0.0] * n

    for cat in _CATEGORIES:
        values = [getattr(p, cat) for p in periods]
        # Split into positive and negative slices — positive segments stack
        # above zero; negative segments stack below.
        pos_values = [v if v > 0 else 0.0 for v in values]
        neg_values = [v if v < 0 else 0.0 for v in values]

        if any(v != 0 for v in pos_values):
            ax.bar(
                x, pos_values, width=bar_width,
                bottom=positive_running, color=_CATEGORY_COLORS[cat],
                edgecolor="none", label=_CATEGORY_LABELS[cat],
            )
            positive_running = [a + b for a, b in zip(positive_running, pos_values)]
        if any(v != 0 for v in neg_values):
            ax.bar(
                x, neg_values, width=bar_width,
                bottom=negative_running, color=_CATEGORY_COLORS[cat],
                edgecolor="none",
                label=_CATEGORY_LABELS[cat] if not any(v != 0 for v in pos_values) else None,
            )
            negative_running = [a + b for a, b in zip(negative_running, neg_values)]

    # Net line overlay
    nets = [p.net for p in periods]
    ax.plot(
        x, nets,
        color=_NET_LINE_COLOR, linewidth=1.6,
        marker="o", markersize=3.5, markerfacecolor=_NET_LINE_COLOR,
        markeredgecolor=_NET_LINE_COLOR,
        label="Net",
    )

    # Zero baseline
    ax.axhline(0, color=_ZERO_LINE_COLOR, linewidth=0.6, alpha=0.6)

    # X-axis tick labels
    labels = [p.label for p in periods]
    max_ticks = 6 if compact else 12
    if n <= max_ticks:
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right",
                           fontsize=6 if compact else 7, color=_TEXT_COLOR)
    else:
        step = max(1, n // max_ticks)
        ticks = list(range(0, n, step))
        ax.set_xticks(ticks)
        ax.set_xticklabels(
            [labels[i] for i in ticks], rotation=45, ha="right",
            fontsize=6 if compact else 7, color=_TEXT_COLOR,
        )

    ax.grid(axis="y", color=_GRID_COLOR, linewidth=0.5, alpha=0.4)

    # Title
    if granularity == "monthly":
        title = f"Monthly Cashflow — Last {n} Month{'s' if n != 1 else ''}"
    else:
        title = f"Annual Cashflow — Last {n} Year{'s' if n != 1 else ''}"
    if not compact:
        ax.set_title(title, color=_TEXT_COLOR, fontsize=10, pad=8, loc="left")

    if not compact:
        # De-duplicate legend entries (matplotlib repeats the label per
        # bar call). Drop duplicates while preserving insertion order.
        handles, lbls = ax.get_legend_handles_labels()
        seen: dict[str, object] = {}
        for h, l in zip(handles, lbls):
            if l and l not in seen:
                seen[l] = h
        legend = ax.legend(
            list(seen.values()), list(seen.keys()),
            fontsize=7, loc="upper right",
            frameon=True, framealpha=0.82, facecolor="#1f1f1f",
            edgecolor=_GRID_COLOR, borderpad=0.4, handlelength=1.4,
            ncol=2,
        )
        legend.get_frame().set_linewidth(0.5)
        for text in legend.get_texts():
            text.set_color(_TEXT_COLOR)

    if compact:
        fig.subplots_adjust(left=0.14, right=0.97, top=0.94, bottom=0.26)
    else:
        fig.subplots_adjust(left=0.10, right=0.97, top=0.88, bottom=0.22)
    return fig
