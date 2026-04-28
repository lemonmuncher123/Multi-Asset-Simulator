import weakref
from matplotlib.figure import Figure

_PALETTE = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
    "#86bcb6", "#8cd17d", "#b6992d", "#499894", "#d37295",
    "#a0cbe8", "#ffbe7d", "#d4a6c8", "#fabfd2", "#d7b5a6",
]

_TEXT_COLOR = "#cccccc"
_TOOLTIP_BG = "#2b2b2b"
_TOOLTIP_BORDER = "#555555"
_TOOLTIP_TEXT = "#e0e0e0"
_EDGE_NORMAL = ("white", 0.5)
_EDGE_HIGHLIGHT = ("#ffffff", 2.5)

_TYPE_LABELS = {
    "stock": "Stock", "etf": "ETF", "crypto": "Crypto",
    "real_estate": "Real Estate", "cash": "Cash", "custom": "Custom",
}


def create_asset_pie_figure(items: list[dict]) -> Figure:
    fig = Figure(figsize=(6, 4), dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)

    if not items:
        ax = fig.add_subplot(111)
        ax.set_facecolor("none")
        ax.text(0.5, 0.5, "No asset data", ha="center", va="center",
                fontsize=14, color="#888888")
        ax.set_axis_off()
        return fig

    labels = [i["name"] for i in items]
    values = [i["value"] for i in items]
    colors = [_PALETTE[idx % len(_PALETTE)] for idx in range(len(items))]

    ax = fig.add_subplot(121)
    ax.set_facecolor("none")
    wedges, texts, autotexts = ax.pie(
        values,
        colors=colors,
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
        startangle=90,
        pctdistance=0.75,
    )
    for t in autotexts:
        t.set_color("white")
        t.set_fontweight("bold")
        t.set_fontsize(9)
    for w in wedges:
        w.set_edgecolor(_EDGE_NORMAL[0])
        w.set_linewidth(_EDGE_NORMAL[1])
    ax.set_aspect("equal")

    legend = ax.legend(
        labels,
        loc="center left",
        bbox_to_anchor=(1.05, 0.5),
        fontsize=9,
        frameon=False,
    )
    for text in legend.get_texts():
        text.set_color(_TEXT_COLOR)

    fig.subplots_adjust(left=0.02, right=0.65, top=0.95, bottom=0.05)

    fig._pie_hover_data = (wedges, items)

    return fig


def connect_pie_hover(canvas):
    fig = canvas.figure
    if not hasattr(fig, "_pie_hover_data"):
        return None

    wedges, items = fig._pie_hover_data
    if not wedges:
        return None

    ax = wedges[0].axes
    annotation = ax.annotate(
        "", xy=(0, 0), xytext=(20, 20), textcoords="offset points",
        bbox=dict(
            boxstyle="round,pad=0.6", fc=_TOOLTIP_BG,
            ec=_TOOLTIP_BORDER, alpha=0.95,
        ),
        color=_TOOLTIP_TEXT, fontsize=9, zorder=10,
    )
    annotation.set_visible(False)

    state = {"idx": None}
    canvas_ref = weakref.ref(canvas)

    def _on_move(event):
        c = canvas_ref()
        if c is None:
            return
        if event.inaxes != ax or event.xdata is None:
            if state["idx"] is not None:
                _reset(state["idx"])
                state["idx"] = None
                annotation.set_visible(False)
                c.draw_idle()
            return

        for i, wedge in enumerate(wedges):
            hit, _ = wedge.contains(event)
            if hit:
                if state["idx"] == i:
                    annotation.xy = (event.xdata, event.ydata)
                    c.draw_idle()
                    return
                if state["idx"] is not None:
                    _reset(state["idx"])
                wedges[i].set_edgecolor(_EDGE_HIGHLIGHT[0])
                wedges[i].set_linewidth(_EDGE_HIGHLIGHT[1])
                state["idx"] = i

                item = items[i]
                type_label = _TYPE_LABELS.get(
                    item["asset_type"],
                    item["asset_type"].replace("_", " ").title(),
                )
                text = (
                    f"{item['name']}\n"
                    f"Type: {type_label}\n"
                    f"Value: ${item['value']:,.2f}\n"
                    f"Allocation: {item['pct']:.1%}"
                )
                annotation.set_text(text)
                annotation.xy = (event.xdata, event.ydata)
                annotation.set_visible(True)
                c.draw_idle()
                return

        if state["idx"] is not None:
            _reset(state["idx"])
            state["idx"] = None
            annotation.set_visible(False)
            c.draw_idle()

    def _reset(idx):
        wedges[idx].set_edgecolor(_EDGE_NORMAL[0])
        wedges[idx].set_linewidth(_EDGE_NORMAL[1])

    return canvas.mpl_connect("motion_notify_event", _on_move)
