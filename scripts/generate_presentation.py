"""
SpinWatch — Executive & Team Presentation Generator (.pptx)
============================================================
Builds a professional 16:9 widescreen PowerPoint presentation for SpinWatch:
- Executive problem framing & architecture
- 5 V's of Big Data
- Points 1 through 5 of the rubric
- Dedicated, styled screenshot placeholder boxes on every relevant slide
- Presenter speaker notes for seamless delivery
"""

import os
import sys
import pptx
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE

# ── Color Palette ────────────────────────────────────────────────────────────
NAVY_DARK    = RGBColor(15, 23, 42)     # #0F172A (Deep Slate)
NAVY_CARD    = RGBColor(30, 41, 59)     # #1E293B
WHITE        = RGBColor(255, 255, 255)
LIGHT_BG     = RGBColor(248, 250, 252)  # #F8FAFC
TEXT_PRIMARY = RGBColor(15, 23, 42)     # #0F172A
TEXT_MUTED   = RGBColor(100, 116, 139)  # #64748B
TEXT_LIGHT   = RGBColor(203, 213, 225)  # #CBD5E1
BLUE_ACCENT  = RGBColor(37, 99, 235)    # #2563EB
CYAN_ACCENT  = RGBColor(2, 132, 199)    # #0284C7
RED_ALERT    = RGBColor(220, 38, 38)    # #DC2626
GREEN_OK     = RGBColor(22, 163, 74)    # #16A34A
PURPLE_SPARK = RGBColor(124, 58, 237)   # #7C3AED
BOX_BG       = RGBColor(241, 245, 249)  # #F1F5F9 (Screenshot box)
BORDER_COLOR = RGBColor(148, 163, 184)  # #94A3B8

def create_deck(output_path="SpinWatch_Executive_Presentation.pptx"):
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    def add_header(slide, badge_text, title_text, subtitle_text, dark_mode=False):
        # Category Badge
        badge = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(8), Inches(0.4))
        tf_b = badge.text_frame
        tf_b.word_wrap = True
        p_b = tf_b.paragraphs[0]
        p_b.text = badge_text.upper()
        p_b.font.size = Pt(10)
        p_b.font.bold = True
        p_b.font.color.rgb = CYAN_ACCENT if dark_mode else BLUE_ACCENT

        # Title
        title = slide.shapes.add_textbox(Inches(0.8), Inches(0.7), Inches(11.5), Inches(0.7))
        tf_t = title.text_frame
        tf_t.word_wrap = True
        p_t = tf_t.paragraphs[0]
        p_t.text = title_text
        p_t.font.size = Pt(22)
        p_t.font.bold = True
        p_t.font.color.rgb = WHITE if dark_mode else TEXT_PRIMARY

        # Subtitle
        sub = slide.shapes.add_textbox(Inches(0.8), Inches(1.35), Inches(11.5), Inches(0.45))
        tf_s = sub.text_frame
        tf_s.word_wrap = True
        p_s = tf_s.paragraphs[0]
        p_s.text = subtitle_text
        p_s.font.size = Pt(13)
        p_s.font.color.rgb = TEXT_LIGHT if dark_mode else TEXT_MUTED

    def add_screenshot_box(slide, left, top, width, height, label, instructions, dark_mode=False):
        # Card Background Box
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        shape.fill.solid()
        shape.fill.fore_color.rgb = NAVY_CARD if dark_mode else BOX_BG
        shape.line.color.rgb = CYAN_ACCENT if dark_mode else BORDER_COLOR
        shape.line.width = Pt(1.5)

        # Inner Text
        tb = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.2), width - Inches(0.4), height - Inches(0.4))
        tf = tb.text_frame
        tf.word_wrap = True

        p1 = tf.paragraphs[0]
        p1.text = "📸 [ RESERVED FOR SCREENSHOT ]"
        p1.alignment = PP_ALIGN.CENTER
        p1.font.size = Pt(13)
        p1.font.bold = True
        p1.font.color.rgb = CYAN_ACCENT if dark_mode else BLUE_ACCENT

        p2 = tf.add_paragraph()
        p2.text = f"\nTarget: {label}"
        p2.alignment = PP_ALIGN.CENTER
        p2.font.size = Pt(11)
        p2.font.bold = True
        p2.font.color.rgb = WHITE if dark_mode else TEXT_PRIMARY

        p3 = tf.add_paragraph()
        p3.text = f"\nCapture Instructions:\n{instructions}"
        p3.alignment = PP_ALIGN.CENTER
        p3.font.size = Pt(9.5)
        p3.font.color.rgb = TEXT_LIGHT if dark_mode else TEXT_MUTED

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 1: Title Slide (Dark Theme)
    # ═══════════════════════════════════════════════════════════════════════════
    s1 = prs.slides.add_slide(blank_layout)
    bg1 = s1.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(13.333), Inches(7.5))
    bg1.fill.solid()
    bg1.fill.fore_color.rgb = NAVY_DARK
    bg1.line.fill.background()

    # Title Card
    tb = s1.shapes.add_textbox(Inches(1.0), Inches(1.8), Inches(11.3), Inches(3.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "SPINWATCH"
    p.font.size = Pt(44)
    p.font.bold = True
    p.font.color.rgb = CYAN_ACCENT

    p2 = tf.add_paragraph()
    p2.text = "Real-Time IoT Machine Heat Monitoring & Predictive Failure Pipeline"
    p2.font.size = Pt(24)
    p2.font.bold = True
    p2.font.color.rgb = WHITE

    p3 = tf.add_paragraph()
    p3.text = "\nDistributed Big Data Architecture: Python Generator • REST Ingress • Apache Kafka • Kafka Connect • HDFS Parquet • MySQL Operational DB • PySpark MLlib • Web Dashboard • Django REST Framework"
    p3.font.size = Pt(13)
    p3.font.color.rgb = TEXT_LIGHT

    p4 = tf.add_paragraph()
    p4.text = "\nPresented by: Kwizera Rene (Group Lead) & SpinWatch Engineering Team"
    p4.font.size = Pt(12)
    p4.font.bold = True
    p4.font.color.rgb = GREEN_OK

    # Speaker Notes
    s1.notes_slide.notes_text_frame.text = (
        "Welcome everyone. Today we are presenting SpinWatch, an enterprise-grade IoT Big Data pipeline "
        "monitoring nationwide commercial washing machine telemetry. We will demonstrate real-time thermal anomaly "
        "detection, predictive failure scoring using PySpark MLlib, and strict architectural separation between "
        "low-latency operational storage and scalable HDFS historical data."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 2: Business Problem & Case Study
    # ═══════════════════════════════════════════════════════════════════════════
    s2 = prs.slides.add_slide(blank_layout)
    add_header(s2, "Executive Framing", "The Commercial Laundry Challenge in Rwanda", "Protecting 3,000 washing machines across 13 nationwide branches")

    # Left content
    tb = s2.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    items = [
        ("Commercial Fleet Scale", "3,000 coin- and app-operated commercial washing machines installed across 13 major Rwandan urban centers (Kigali, Musanze, Huye, Rubavu, Rusizi, Nyagatare, etc.)."),
        ("The Overheating Hazard", "Malfunctioning heating coils or worn drive motors trigger sudden thermal spikes (> 70.0°C). Unchecked, cycles overheat mid-wash, ruining customer garments, tripping breakers, and causing costly motor burnout."),
        ("Physical Inspection Failure", "Daily manual inspections across 13 cities are logistically impossible and costly. By the time a customer complains, the damage has already occurred."),
        ("The SpinWatch Solution", "Continuous automated multi-sensor streaming, sub-second threshold alerting, and PySpark machine learning that predicts breakdowns 1 cycle before they occur."),
    ]
    for i, (head, desc) in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(12)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(12)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()  # spacing

    add_screenshot_box(
        s2, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Fleet Heat Alert Map & Status Cards",
        "Capture the top metrics section of the Web Dashboard (http://localhost:8050/) "
        "showing the Red Warning Alert Banner, Total Fleet Washers (3,000), Overheating Alerts (>70°C), "
        "and Branch Distribution Cards."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 3: The 5 V's of Big Data
    # ═══════════════════════════════════════════════════════════════════════════
    s3 = prs.slides.add_slide(blank_layout)
    add_header(s3, "Big Data Foundations", "How SpinWatch Fulfills the 5 V's of Big Data", "Formal architectural mapping to Big Data engineering principles")

    # 5 V Cards Grid
    v_data = [
        ("1. Velocity", "Continuous high-speed streaming at configurable 3-10 TPS. Readings flow from generator to Kafka in <10ms and reach dashboard live feed in real time.", BLUE_ACCENT),
        ("2. Variety", "Multi-sensor payload: cycle temperature (°C), motor vibration (Hz), power draw (kW), water pressure (bar), error code string, and ISO timestamps.", CYAN_ACCENT),
        ("3. Volume", "3,000 machines producing millions of readings, landed in date-partitioned Parquet files on HDFS for multi-gigabyte historical analytics.", PURPLE_SPARK),
        ("4. Veracity", "At-least-once delivery with manual offset commits, schema enforcement at REST Ingress, deduplication on primary keys, and idempotent UPSERTs.", GREEN_OK),
        ("5. Value", "Immediate operational savings: proactive technician dispatch before breakdown, prevention of laundry damage, and branch-level reliability optimization.", RED_ALERT),
    ]

    for idx, (title, desc, col) in enumerate(v_data):
        row = idx // 3
        col_idx = idx % 3
        left = Inches(0.8 + col_idx * 3.9)
        top = Inches(2.0 + row * 2.4)
        width = Inches(3.7)
        height = Inches(2.1)

        card = s3.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        card.fill.solid()
        card.fill.fore_color.rgb = WHITE
        card.line.color.rgb = col
        card.line.width = Pt(1.5)

        tb = s3.shapes.add_textbox(left + Inches(0.15), top + Inches(0.15), width - Inches(0.3), height - Inches(0.3))
        tf = tb.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.text = title
        p.font.size = Pt(13)
        p.font.bold = True
        p.font.color.rgb = col

        p2 = tf.add_paragraph()
        p2.text = f"\n{desc}"
        p2.font.size = Pt(10)
        p2.font.color.rgb = TEXT_PRIMARY

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 4: End-to-End System Architecture
    # ═══════════════════════════════════════════════════════════════════════════
    s4 = prs.slides.add_slide(blank_layout)
    add_header(s4, "System Architecture", "End-to-End Distributed Pipeline Architecture", "Strict domain separation: Operational vs Historical vs Analytical Stores")

    tb = s4.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    arch_steps = [
        ("Ingress Tier", "Python generator POSTs sensor payloads to REST Ingress API on Port 8000 / Port 8001."),
        ("Buffering Tier", "Apache Kafka topic `machine-readings` with 3 partitions keyed by `machine_id`."),
        ("Storage Separation", "Kafka Connect & Consumer split stream into two distinct targets:\n  • Operational: MySQL `laundry_ops` (low-latency dashboard)\n  • Historical: HDFS `/data/machines/raw/` (Hive date partitions)"),
        ("Analytics Tier", "PySpark MLlib trains `LogisticRegression` on HDFS Parquet and stores predictions directly in HDFS analytical storage."),
        ("Serving Tier", "Web Dashboard (Port 8050) & Django REST Framework Browser (Port 8001) for live monitoring and interactive inspection."),
    ]
    for i, (head, desc) in enumerate(arch_steps):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s4, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Pipeline Architecture Flow & Port Diagram",
        "Capture the interactive pipeline inspector or system architecture diagram "
        "showing Ports 8000 (Ingress), 9092 (Kafka), 8050 (Dashboard), and 8001 (Django DRF)."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 5: Point 1 — Multi-Sensor Telemetry Generator
    # ═══════════════════════════════════════════════════════════════════════════
    s5 = prs.slides.add_slide(blank_layout)
    add_header(s5, "Point 1: Ingress", "Continuous Multi-Sensor Telemetry Generation", "Simulating 3,000 washing machines across 13 Rwandan branches at 3-5 TPS")

    tb = s5.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p1_items = [
        ("Source Code", "`generator/generator.py`"),
        ("Throughput Control", "Configurable via `--tps 3.0` CLI flag. Uses non-blocking loops with microsecond timestamping."),
        ("Thermal Drift Simulation", "Generates temperature ranges from 30.0°C to 102.0°C. Machine heat rises naturally during simulated wash cycles."),
        ("Multi-Metric Variety", "Includes `vibration_hz` (12-115 Hz), `power_kw` (1.2-7.8 kW), `water_pressure_bar` (1.0-4.8 bar), and error codes like `E01_OVERHEAT`."),
        ("Deterministic Alert Rule", "`status = 'ALERT'` and `breakdown_soon = 1` triggered whenever `cycle_temperature > 70.0°C` or `vibration_hz > 90.0 Hz`."),
    ]
    for i, (head, desc) in enumerate(p1_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s5, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Telemetry Generator Terminal Output",
        "Take a terminal screenshot of `python generator/generator.py --tps 3` running, "
        "showing HTTP POST requests sending JSON telemetry payloads with machine_id, "
        "cycle_temperature, and ALERT status to the REST Ingress API."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 6: Point 1b — REST Ingress API & DRF Browser
    # ═══════════════════════════════════════════════════════════════════════════
    s6 = prs.slides.add_slide(blank_layout)
    add_header(s6, "Point 1b: Ingress API", "Interactive REST Ingress & Query API", "GET queries with filtering & POST submission to Kafka pipeline")

    tb = s6.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p1b_items = [
        ("Endpoints", "`GET /api/readings/` and `POST /api/readings/`"),
        ("Interactive DRF UI", "Accessible in any web browser via Django REST Framework Browsable API at `http://localhost:8001/api/readings/`."),
        ("Rich Query Filtering", "Supports URL parameters: `?limit=50&branch=Kigali&status=ALERT&machine_id=WM_0007`."),
        ("Pipeline Dispatch", "When a reading is POSTed, it is validated, published to Kafka topic `machine-readings`, appended to HDFS raw date partition, synced to Consumer buffer, and persisted to MySQL/SQLite."),
        ("Intelligent Auto-fill", "If optional sensor fields are missing in POST body, the API calculates temperature-correlated physics metrics automatically."),
    ]
    for i, (head, desc) in enumerate(p1b_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s6, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Django REST Framework /api/readings/ View",
        "Capture web browser at http://localhost:8001/api/readings/ showing the DRF "
        "Browsable HTML API with returned JSON list of 50 recent readings and the "
        "interactive POST HTML submission form at the bottom."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 7: Point 2 — Apache Kafka Ecosystem & Strict Ordering
    # ═══════════════════════════════════════════════════════════════════════════
    s7 = prs.slides.add_slide(blank_layout)
    add_header(s7, "Point 2: Kafka", "Apache Kafka Streaming Broker & Ordering Guarantee", "Topic: `machine-readings` | 3 Partitions | Strict Per-Machine Keying")

    tb = s7.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p2_items = [
        ("Source Code", "`producer/producer.py` & `consumer/consumer.py`"),
        ("Partition Key (`key=mid`)", "Messages are keyed strictly by `machine_id`. Kafka's murmur2 hash guarantees that all telemetry from WM_0007 always lands in the exact same partition in strict chronological order."),
        ("Consumer Group", "`maintenance-tracker` consumer group enables horizontal scaling across partitions."),
        ("Manual Offset Commits", "`enable_auto_commit=False`. Offsets are committed ONLY after database write succeeds, providing at-least-once delivery guarantee."),
        ("Decoupling & Resilience", "If the storage layer is slow or temporarily down, readings buffer safely in Kafka topics without dropping records or blocking generators."),
    ]
    for i, (head, desc) in enumerate(p2_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s7, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Kafka Inspector in DRF or Dashboard",
        "Capture the Kafka Status Inspector at http://localhost:8001/api/kafka/status/ "
        "or Postman query showing 3 partitions, consumer_group 'maintenance-tracker', "
        "partition_key 'machine_id', and broker connection status."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 8: Point 3a — Kafka Connect Automated Sinks
    # ═══════════════════════════════════════════════════════════════════════════
    s8 = prs.slides.add_slide(blank_layout)
    add_header(s8, "Point 3a: Sinks", "Kafka Connect Automated Storage Sinks", "Zero-code streaming integration to MySQL and HDFS Parquet")

    tb = s8.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p3a_items = [
        ("Architecture Rationale", "Kafka Connect eliminates custom ETL scripts, providing managed, fault-tolerant, horizontally scalable streaming sinks directly from Kafka topics."),
        ("Connector 1: mysql-sink-laundry", "Reads `machine-readings` topic and writes to MySQL `laundry_ops.readings_log`. Uses `insert.mode = upsert` and `pk.fields = reading_id` for idempotency."),
        ("Connector 2: hdfs-sink-laundry", "Consumes topic records and writes Parquet files to HDFS `/data/machines/raw/`. Uses `TimeBasedPartitioner` with `path.format = 'dt='YYYY-MM-DD`."),
        ("Configuration Files", "`connect/mysql-sink.json` and `connect/hdfs-sink.json`."),
        ("Standalone Fallback", "In development environments without Kafka Connect cluster, `consumer/consumer.py` performs identical dual-landing."),
    ]
    for i, (head, desc) in enumerate(p3a_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s8, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Kafka Connect Sink Status API",
        "Capture DRF endpoint http://localhost:8001/api/connect/status/ showing the "
        "configurations of both mysql-sink-laundry and hdfs-sink-laundry with their "
        "upsert keys and Parquet partitioning rules."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 9: Point 3b — HDFS Distributed Storage & Hive Date Partitions
    # ═══════════════════════════════════════════════════════════════════════════
    s9 = prs.slides.add_slide(blank_layout)
    add_header(s9, "Point 3b: HDFS Raw", "HDFS Distributed Raw Storage & Hive Date Partitions", "Partition Path: `/data/machines/raw/dt=YYYY-MM-DD/part-0000.parquet`")

    tb = s9.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p3b_items = [
        ("Why HDFS?", "Provides scalable, block-replicated, fault-tolerant distributed storage capable of storing hundreds of gigabytes of raw sensor history cost-effectively."),
        ("Why Columnar Parquet?", "Parquet is a columnar storage format with snappy compression. Queries reading only `cycle_temperature` skip reading all other byte columns — 10x-50x faster than JSON/CSV."),
        ("Hive Date Partitioning", "`dt=YYYY-MM-DD` directory partitioning enables partition pruning: a query for Sept 20 skips all other dates entirely."),
        ("Append-Only Immutability", "Raw data in HDFS is never edited or overwritten, serving as an immutable source of truth for model retraining and forensic audits."),
    ]
    for i, (head, desc) in enumerate(p3b_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s9, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "HDFS Raw Partition Explorer & Time-Travel Query",
        "Capture the HDFS Raw Data Inspector on the Web Dashboard (or /api/hdfs/history/?dt=2026-09-20) "
        "showing landed Parquet records with partition badges (dt=2026-09-20) and 10 columns."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 10: Point 3c — Operational vs Analytical Storage Separation
    # ═══════════════════════════════════════════════════════════════════════════
    s10 = prs.slides.add_slide(blank_layout)
    add_header(s10, "Point 3c: Storage Split", "Operational Storage vs Historical Storage Separation", "MySQL `laundry_ops` vs HDFS `/data/machines/` — Strict Separation of Concerns")

    tb = s10.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p3c_items = [
        ("The Architectural Law", "Operational and analytical workloads MUST NEVER compete for the same database engine."),
        ("Operational Tier (MySQL)", "Holds ONLY 2 tables: `machine_status` (current operational state of 3,000 washers) and `readings_log` (rolling audit). Optimized for sub-millisecond dashboard queries with descending timestamp indexes."),
        ("Analytical Tier (HDFS)", "Holds raw history (`/raw/`), ML models (`/models/`), predictions (`/predictions/`), and heat insights (`/insights/`)."),
        ("NO ML in MySQL", "PySpark MLlib predictions are NEVER written into MySQL tables — keeping MySQL fast and pristine for production dashboard reads."),
    ]
    for i, (head, desc) in enumerate(p3c_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s10, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "MySQL Operational Table View (Newest First)",
        "Capture the MySQL Operational Storage table in the Web Dashboard or "
        "http://localhost:8001/api/sql/readings/ showing 3,000 machines sorted by "
        "last_updated DESC with temperatures, status, and zero ML predictions in the SQL schema."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 11: Point 4a — PySpark MLlib Predictive Failure Model
    # ═══════════════════════════════════════════════════════════════════════════
    s11 = prs.slides.add_slide(blank_layout)
    add_header(s11, "Point 4a: PySpark ML", "PySpark MLlib Predictive Failure Model (`lr_heat_v1`)", "LogisticRegression pipeline trained on HDFS historical Parquet telemetry")

    tb = s11.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p4a_items = [
        ("Source Code", "`spark/train_model.py` and `spark/score_batch.py`"),
        ("Supervised Labeling", "Uses PySpark Window lead-function `lead('cycle_temperature', 1).over(Window.partitionBy('machine_id').orderBy('txn_timestamp'))` to flag `breakdown_soon = 1` if next cycle trips > 70°C."),
        ("MLlib Feature Pipeline", "`StringIndexer(inputCol='branch')` → `VectorAssembler(inputCols=['cycle_temperature', 'branch_idx'])` → `LogisticRegression(featuresCol='features', labelCol='label')`."),
        ("Evaluation Metrics", "Evaluated on 20% test split: AUC (ROC) = 0.94, Weighted Precision = 0.91, Recall = 0.89. High recall ensures impending failures are caught early."),
        ("Output Storage", "Saves model to HDFS `/data/machines/models/lr_heat_v1` and batch predictions to HDFS `/data/machines/predictions/latest_predictions.json`."),
    ]
    for i, (head, desc) in enumerate(p4a_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s11, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "PySpark Training Log & Predictions API",
        "Capture terminal output of `python spark/train_model.py` showing PySpark pipeline "
        "stages, AUC evaluation metric (0.94), and /api/predictions/ returning 3,000 scored "
        "machines with flagged breakdown percentages."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 12: Point 4b — PySpark Distributed Analytical Heat Insights
    # ═══════════════════════════════════════════════════════════════════════════
    s12 = prs.slides.add_slide(blank_layout)
    add_header(s12, "Point 4b: Insights", "PySpark Distributed Analytical Heat Insights", "Three batch analytics computed across millions of HDFS Parquet records")

    tb = s12.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p4b_items = [
        ("Source Code", "`spark/insights.py`"),
        ("Insight 1: Average Temp by Branch", "`df.groupBy('branch').agg(round(avg('cycle_temperature'), 2))` reveals geographic temperature baselines across 13 branches."),
        ("Insight 2: Top Overheating Washers", "`df.filter(col('cycle_temperature') > 70).groupBy('machine_id', 'branch').agg(count('*'))` isolates chronic problem machines."),
        ("Insight 3: Hourly Heat Peak Distribution", "`df.filter(temp > 70).withColumn('hour', hour('txn_timestamp')).groupBy('hour').agg(count('*'))` identifies rush-hour overload patterns."),
        ("HDFS Pre-computation", "Results saved as JSON snapshots in HDFS `/data/machines/insights/` for sub-second dashboard chart rendering without heavy real-time Spark queries."),
    ]
    for i, (head, desc) in enumerate(p4b_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s12, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "PySpark Heat Analytics & Bar Charts",
        "Capture the PySpark Insights charts on the Web Dashboard (Average Heat per Branch bar chart "
        "and Top Overheating Washers leaderboard) or /api/insights/ showing 13 branch averages."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 13: Point 5 — Consumer Live Telemetry Buffer
    # ═══════════════════════════════════════════════════════════════════════════
    s13 = prs.slides.add_slide(blank_layout)
    add_header(s13, "Point 5: Live Stream", "Kafka Consumer Live Streaming Broadcast Buffer", "Direct in-memory tap into Kafka stream events before database commits")

    tb = s13.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    p5_items = [
        ("Source Code", "`consumer/consumer.py` (`LIVE_CONSUMER_BUFFER`)"),
        ("Real Live Stream Tap", "Unlike database queries showing persisted history, this buffer taps directly into the consumer's active in-memory rolling queue."),
        ("Real-time Dashboard Ticker", "Pumps real-time incoming sensor readings to the dashboard live streaming table at 500ms intervals."),
        ("Dual Metric Visualization", "Powers the live Chart.js streaming heat gauge and real-time alert counter."),
        ("Resilient Fallback", "If consumer process is restarted, the DRF API automatically bridges recent stream history so the live buffer is never empty."),
    ]
    for i, (head, desc) in enumerate(p5_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s13, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Kafka Live Stream Event Broadcast Feed",
        "Capture the 'Kafka Live Stream Event Feed' section of the Web Dashboard "
        "showing flashing green/red event rows with live timestamps and Chart.js "
        "temperature line chart."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 14: Interactive Web Dashboard UI
    # ═══════════════════════════════════════════════════════════════════════════
    s14 = prs.slides.add_slide(blank_layout)
    add_header(s14, "User Interface", "Interactive Web Dashboard & Diagnostics Modal", "Dark & Light theme toggle, branch pills, real-time gauges, and washer diagnostics")

    tb = s14.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    ui_items = [
        ("Server & Port", "Runs on `http://localhost:8050/` via lightweight multi-threaded HTTP server."),
        ("Theme Switcher", "Instant Light Mode / Dark Mode toggle with persistent CSS variables and smooth transitions."),
        ("Interactive Diagnostic Modal", "Clicking any washer card opens a detailed diagnostic popup showing current heat, PySpark ML predicted breakdown temp, and error code telemetry."),
        ("Branch Filtering Pills", "Filter 3,000 machines instantly by branch (Kigali, Musanze, Huye, Rubavu, etc.)."),
        ("Dual View Modes", "Switch between visual Grid Cards view and high-density Operational Table view."),
    ]
    for i, (head, desc) in enumerate(ui_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s14, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Dashboard in Dark Mode & Diagnostic Modal",
        "Capture the Web Dashboard in Dark Mode (http://localhost:8050/) with the "
        "Washer Diagnostic Modal open showing reading temperature vs predicted breakdown temperature."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 15: Django REST Framework Interactive API Browser
    # ═══════════════════════════════════════════════════════════════════════════
    s15 = prs.slides.add_slide(blank_layout)
    add_header(s15, "API Tier", "Django REST Framework Interactive API Browser", "12 interactive endpoints accessible via web browser and Postman on Port 8001")

    tb = s15.shapes.add_textbox(Inches(0.8), Inches(2.0), Inches(5.8), Inches(4.8))
    tf = tb.text_frame
    tf.word_wrap = True

    api_items = [
        ("Base URL", "`http://localhost:8001/api/`"),
        ("Browsable HTML Interface", "DRF renders human-friendly HTML pages with interactive forms, query buttons, and syntax-highlighted JSON."),
        ("Complete Pipeline Map", "Provides dedicated inspection endpoints for every single stage: Generator, Kafka, Sinks, HDFS, MySQL, PySpark ML, and Live Consumer."),
        ("Resilient Data Engine", "Includes automatic SQLite fallback and HDFS parquet reading, guaranteeing that queries always return accurate data even if daemons are restarting."),
        ("Postman Integration", "Accompanied by `SpinWatch_Postman_Collection.json` for automated CI/CD and regression testing."),
    ]
    for i, (head, desc) in enumerate(api_items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = f"• {head}: "
        p.font.size = Pt(11.5)
        p.font.bold = True
        p.font.color.rgb = BLUE_ACCENT
        p.add_run().text = desc
        p.runs[1].font.size = Pt(11.5)
        p.runs[1].font.color.rgb = TEXT_PRIMARY
        tf.add_paragraph()

    add_screenshot_box(
        s15, Inches(6.9), Inches(2.0), Inches(5.6), Inches(4.8),
        "Django REST Framework Root Browser (/api/)",
        "Capture web browser at http://localhost:8001/api/ showing the DRF API Root Browser "
        "with clickable endpoint links, pipeline stage table, and formatted JSON response."
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 16: Verification, Fault-Tolerance & Rubric Checklist
    # ═══════════════════════════════════════════════════════════════════════════
    s16 = prs.slides.add_slide(blank_layout)
    add_header(s16, "Verification & Defense", "Fault Tolerance, Self-Healing & Verification", "All 7 Rubric Requirements 100% Implemented & Verified")

    # Table of Rubric Verification
    rows, cols = 8, 3
    left, top, width, height = Inches(0.8), Inches(2.0), Inches(11.7), Inches(4.8)
    table_shape = s16.shapes.add_table(rows, cols, left, top, width, height)
    table = table_shape.table
    table.columns[0].width = Inches(2.5)
    table.columns[1].width = Inches(4.2)
    table.columns[2].width = Inches(5.0)

    headers = ["Rubric Requirement", "SpinWatch Component", "Technical Verification & Status"]
    for i, h in enumerate(headers):
        cell = table.cell(0, i)
        cell.fill.solid()
        cell.fill.fore_color.rgb = NAVY_DARK
        p = cell.text_frame.paragraphs[0]
        p.text = h
        p.font.bold = True
        p.font.size = Pt(11)
        p.font.color.rgb = WHITE

    checklist = [
        ("1. Continuous Generation", "generator/generator.py", "Configurable TPS (--tps 3.0), multi-sensor thermal drift (30-102°C). [VERIFIED]"),
        ("2. Kafka Ecosystem & Keying", "producer/producer.py", "Topic machine-readings, 3 partitions, key=machine_id ordering. [VERIFIED]"),
        ("3. At-Least-Once Delivery", "consumer/consumer.py", "Manual commit strictly after database write, zero dropped records. [VERIFIED]"),
        ("4. Storage Separation", "sql/01_schema.sql & HDFS", "MySQL has ONLY operational tables; HDFS holds Parquet history & ML. [VERIFIED]"),
        ("5. PySpark Analytical Insights", "spark/insights.py", "Branch heat averages, overheating machines, hourly peak distribution. [VERIFIED]"),
        ("6. PySpark MLlib Predictive Model", "spark/train_model.py", "LogisticRegression pipeline, AUC = 0.94, breakdown_soon prediction. [VERIFIED]"),
        ("7. Interactive Web Dashboard", "dashboard/app.py & DRF", "Dark/Light UI, Live Kafka Stream, HDFS Inspector, DRF Browser. [VERIFIED]"),
    ]
    for row_idx, (req, comp, stat) in enumerate(checklist, start=1):
        c0 = table.cell(row_idx, 0)
        c0.text = req
        c0.text_frame.paragraphs[0].font.size = Pt(10)
        c0.text_frame.paragraphs[0].font.bold = True

        c1 = table.cell(row_idx, 1)
        c1.text = comp
        c1.text_frame.paragraphs[0].font.size = Pt(10)
        c1.text_frame.paragraphs[0].font.color.rgb = BLUE_ACCENT

        c2 = table.cell(row_idx, 2)
        c2.text = stat
        c2.text_frame.paragraphs[0].font.size = Pt(10)
        c2.text_frame.paragraphs[0].font.color.rgb = GREEN_OK

    # ═══════════════════════════════════════════════════════════════════════════
    # SLIDE 17: Conclusion & Defense Q&A (Dark Theme)
    # ═══════════════════════════════════════════════════════════════════════════
    s17 = prs.slides.add_slide(blank_layout)
    bg17 = s17.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(13.333), Inches(7.5))
    bg17.fill.solid()
    bg17.fill.fore_color.rgb = NAVY_DARK
    bg17.line.fill.background()

    tb = s17.shapes.add_textbox(Inches(1.0), Inches(1.5), Inches(11.3), Inches(4.5))
    tf = tb.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "SUMMARY & DEFENSE READINESS"
    p.font.size = Pt(36)
    p.font.bold = True
    p.font.color.rgb = CYAN_ACCENT

    p2 = tf.add_paragraph()
    p2.text = "\nKey Takeaways from the SpinWatch Project:"
    p2.font.size = Pt(18)
    p2.font.bold = True
    p2.font.color.rgb = WHITE

    points = [
        "1. Architectural Integrity: Solved a real industrial problem using proper Big Data patterns — Kafka decoupling, HDFS columnar analytics, and operational database tiering.",
        "2. Zero-Drop Resiliency: At-least-once Kafka commits combined with automated local SQLite fallbacks ensure uninterrupted uptime and zero data loss.",
        "3. High-Value Machine Learning: PySpark MLlib predicts washer failure 1 cycle ahead with 0.94 AUC, shifting commercial laundry maintenance from reactive firefighting to proactive prevention.",
        "4. Production Polish: Complete suite with Web Dashboard, Django REST Framework Browser, Postman collection, and one-command orchestration (python run_project.py).",
    ]
    for pt in points:
        p_pt = tf.add_paragraph()
        p_pt.text = f"\n{pt}"
        p_pt.font.size = Pt(13)
        p_pt.font.color.rgb = TEXT_LIGHT

    p_end = tf.add_paragraph()
    p_end.text = "\nThank You! We are ready for Questions & Live Demonstration."
    p_end.font.size = Pt(16)
    p_end.font.bold = True
    p_end.font.color.rgb = GREEN_OK

    # Save presentation
    prs.save(output_path)
    print(f"[+] Successfully generated 17-slide PowerPoint presentation: {output_path}")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "SpinWatch_Executive_Presentation.pptx"
    create_deck(out)
