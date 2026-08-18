# Python Data Pipeline Engineering — Omnichannel Retail ETL

Pipeline สำหรับแปลงข้อมูลคำสั่งซื้อ 3 batch (`orders_batch_1-3`) ที่มีความผิดปกติ
ให้กลายเป็น Star Schema ใน SQLite แบบ **Idempotent** (รันซ้ำไม่เพิ่มข้อมูล) และ
**Incremental** (โหลดเฉพาะข้อมูลใหม่/อัปเดต) โดยไม่แก้ไขไฟล์ต้นฉบับ

## 1. วิธีติดตั้งและวิธีรัน

```bash
pip install pandas openpyxl
python pipeline.py
```

สคริปต์จะ:
1. ลบ `retail_dw.db` เดิม (ถ้ามี) เพื่อให้รันซ้ำจากศูนย์ได้อย่างสะอาด
2. รัน 4 รอบตามลำดับ: `orders_batch_1` → `orders_batch_1` (รันซ้ำ ทดสอบ Idempotency) →
   `orders_batch_2` → `orders_batch_3`
3. พิมพ์ `pipeline_run_log` และสรุป KPI ทางหน้าจอ
4. Export `quarantine.csv` และ `pipeline_run_log.csv` จากฐานข้อมูล

ไฟล์ต้นทาง (`Python_Data_Pipeline_Lab_Dataset__1_.xlsx`) จะถูก**อ่านเท่านั้น**
ไม่มีจุดใดในโค้ดที่เขียนกลับไปยังไฟล์นี้

## 2. โครงสร้าง Star Schema

Grain ของ `fact_sales` = **หนึ่งรายการขายสินค้าที่ผ่านการตรวจสอบแล้ว ต่อ order_id**

| ตาราง | Key | คอลัมน์หลัก |
| --- | --- | --- |
| `dim_customer` | `customer_key` (surrogate, autoincrement) | `customer_id` (unique), `customer_name`, `province`, `segment` |
| `dim_product` | `product_key` (surrogate) | `product_id` (unique), `product_name`, `category`, `active_flag` |
| `dim_date` | `date_key` (YYYYMMDD) | `full_date`, `day`, `month`, `quarter`, `year` — สร้างเฉพาะวันที่ที่มีธุรกรรมจริง (ไม่ pre-populate ทั้งปี) |
| `fact_sales` | `order_id` (PRIMARY KEY) | `date_key`, `customer_key`, `product_key` (FK), `quantity`, `unit_price`, `discount_pct`, `gross_amount`, `net_amount`, `payment_method`, `sales_channel`, บวก `updated_at`/`source_batch`/`loaded_at` เพื่อ trace ย้อนกลับ |
| `quarantine` | unique (`order_id`,`source_batch`) | สำเนาคอลัมน์ดิบ + `reason_code` + `source_batch` |
| `pipeline_run_log` | `run_id` | `batch`, `started_at`, `ended_at`, `rows_read`, `rows_valid`, `rows_rejected`, `rows_duplicated`, `rows_loaded`, `status` |

`PRAGMA foreign_keys = ON` และมี FK constraint จริงจาก `fact_sales` ไปยัง 3 มิติ —
แถวที่อ้างอิง customer/product ที่ไม่มีอยู่จริงจะถูก**คัดออกไป quarantine ก่อน**เสมอ จึงไม่มีทาง insert ให้ FK พังได้

## 3. Data Quality Rules → `reason_code`

ทุกแถวถูกตรวจสอบ**ทุกกฎพร้อมกัน** (ไม่ short-circuit) แล้วรวม reason_code ที่พบทั้งหมดด้วย `;`

| reason_code | เงื่อนไข |
| --- | --- |
| `MISSING_CUSTOMER_ID` / `CUSTOMER_NOT_FOUND` | customer_id ว่าง หรือไม่พบใน `dim_customer` |
| `MISSING_PRODUCT_ID` / `PRODUCT_NOT_FOUND` | product_id ว่าง หรือไม่พบใน `dim_product` |
| `PRODUCT_INACTIVE` | product_id พบ แต่ `active_flag = 'N'` (สมมติฐาน: สินค้าที่ยกเลิกขายแล้วไม่ควรมียอดขายใหม่) |
| `INVALID_QUANTITY` | แปลงเป็นตัวเลขไม่ได้ / ไม่ใช่จำนวนเต็ม / อยู่นอกช่วง 1–20 |
| `INVALID_UNIT_PRICE` | แปลงเป็นตัวเลขไม่ได้ หรือ ≤ 0 |
| `INVALID_DISCOUNT_PCT` | แปลงเป็นตัวเลขไม่ได้ หรืออยู่นอกช่วง 0–100 |
| `INVALID_ORDER_DATETIME` | parse วันที่ไม่ได้ (เช่น `31/02/2026`, `not-a-date`) |
| `INVALID_UPDATED_AT` | parse วันที่ไม่ได้ (ใช้เป็น key สำหรับ dedup/upsert) |
| `INVALID_PAYMENT_METHOD` / `INVALID_SALES_CHANNEL` | หลัง normalize แล้วยังไม่ตรงค่าที่อนุมัติ |

**Normalize:** `payment_method` ตัด space/ทำเป็น lower ก่อนแล้ว map เป็น 4 ค่ามาตรฐาน
(`Cash`, `Credit Card`, `Bank Transfer`, `PromptPay`) — แก้ปัญหา `credit card` vs `Credit Card`
`sales_channel` map `E-Commerce → Online` ตามกฎใน data_dictionary

**Dedup:** ทำ**หลัง**ผ่าน validation แล้วเท่านั้น เก็บระเบียนที่ `updated_at` ล่าสุดต่อ `order_id`
(แถวที่ invalid จะไม่ถูกเอาไป dedup — มันไปลง quarantine ทั้งหมด ไม่ว่าจะซ้ำหรือไม่)

## 4. Idempotency & Incremental Loading (Task 4)

`upsert_fact()` เทียบ `order_id` กับที่มีอยู่แล้วใน `fact_sales`:

- **ไม่เคยมี** → INSERT (นับเป็น loaded)
- **มีอยู่แล้ว และ `updated_at` ใหม่กว่า** → UPDATE (ข้อมูลอัปเดตล่าสุดชนะ, นับเป็น loaded)
- **มีอยู่แล้ว และ `updated_at` เท่ากับ/เก่ากว่า** → ข้าม (idempotent no-op)

หลักฐาน 4 รอบการรัน (จากการรันจริง, ดูไฟล์แนบ `pipeline_run_log.csv`):

| run | batch | rows_read | valid | rejected | duplicated | loaded | fact_sales หลังรอบนี้ |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | orders_batch_1 | 420 | 373 | 47 | 0 | 373 | 373 |
| 2 | orders_batch_1 (ซ้ำ) | 420 | 373 | 47 | 0 | **0** | 373 (ไม่เพิ่ม ✅) |
| 3 | orders_batch_2 | 424 | 366 | 58 | 1 | 365 | 738 |
| 4 | orders_batch_3 | 424 | 370 | 54 | 3 | 366 | **1,103** |

**หลักฐานเชิงลึกของ incremental correctness:** ชุดข้อมูลมีแถวคาบเกี่ยว batch โดยตั้งใจ —
`O000411` ปรากฏทั้งใน batch_1 (`updated_at=02-09`) และ batch_2 (`updated_at=03-16`, ใหม่กว่า)
→ ระบบ **UPDATE** ให้ถูกต้องเป็นเวอร์ชันล่าสุด
ส่วน `O000831` ปรากฏใน batch_2 (`updated_at=04-22`) และ batch_3 (`updated_at=03-17`, **เก่ากว่า**)
→ ระบบ **ข้าม** การอัปเดตของ batch_3 ไว้อย่างถูกต้อง ป้องกันไม่ให้ข้อมูลใหม่ถูกทับด้วยข้อมูลเก่า
(นี่คือเหตุผลที่ผลรวม `rows_loaded` สะสม = 1,104 ครั้ง แต่แถวสุดท้ายใน `fact_sales` = 1,103 แถว
— ต่างกัน 1 เพราะ `O000411` ถูกนับเป็น "loaded" สองครั้ง: insert ใน batch_1 แล้ว update ใน batch_2)

**Acceptance test ข้อ 7** (`read = valid + rejected` ก่อน dedup): ตรวจสอบได้ตรงทุกแถวในตารางด้านบน
(เช่น batch_2: 424 = 366 + 58)

## 5. Orchestration & Error Handling (Task 5)

`run_pipeline(config)` เรียก extract → transform → validate → load ต่อ 1 batch ต่อครั้ง:

- **แถวผิดพลาดระดับเดียว** → ไปที่ quarantine, ไม่กระทบแถวอื่น (`error_mode="quarantine"`, ค่า default)
- **Batch ทั้งก้อนใช้งานไม่ได้** (เช่น อ่านไฟล์/ชีทไม่ได้) → `try/except` จับไว้ บันทึกสถานะ `FAILED` ใน
  `pipeline_run_log` และ**ไม่ rollback ข้อมูลจาก batch ก่อนหน้าที่ commit ไปแล้ว**
  (ทดสอบแล้ว: ยิง batch ที่ไม่มีอยู่จริงเข้าไป → `fact_sales` ยังคงมี 1,103 แถวเท่าเดิม ไม่มีข้อมูลหาย)
- การเขียน `dim_date` + `fact_sales` + `quarantine` ของแต่ละ batch ทำใน **1 transaction เดียวกัน**
  (all-or-nothing ต่อ batch) เพื่อไม่ให้ batch ค้างอยู่ในสถานะโหลดครึ่งเดียว

### KPI สรุป (สะสมจากการรันจริงทั้ง 4 รอบ)

```
rows read      : 1,688
rows valid     : 1,482
rows rejected  : 206      (โดยหลัง collapse ข้อมูลซ้ำที่ผิดปกติเหมือนกันทุกจุด เหลือ 157 แถวใน quarantine)
rows duplicated: 4
rows loaded    : 1,104
fact_sales รวมสุดท้าย: 1,103 แถว
ยอดขายสุทธิรวม (net sales): 2,697,350.29
```

> หมายเหตุ `quarantine.csv` มี unique key เป็น `(order_id, source_batch)` — ถ้ารัน batch เดิมซ้ำ
> (เช่น batch_1 รอบ 2) แถวที่ถูก reject ซ้ำจะถูก **REPLACE** ไม่ใช่ append ซ้ำ ทำให้ quarantine
> เป็น idempotent เหมือนกับ fact_sales ไฟล์ `quarantine.csv` จึงมี 157 แถว (ไม่ใช่ 206) — ส่วน
> 206 คือจำนวน "เหตุการณ์ reject ทั้งหมดที่อ่านเจอ" ซึ่งเป็นตัวเลขที่ใช้ตรวจ acceptance test ข้อ 7

## 6. สมมติฐานสำคัญที่ใช้ในการออกแบบ

- **ไม่เติมค่าที่ขาด/ผิดจากแหล่งอื่น** — เช่น `unit_price` ของ order ที่ว่าง/ผิด จะไม่ไปหยิบราคาปัจจุบันจาก
  `dim_product` มาแทน แต่ส่งเข้า quarantine ตรง ๆ เพื่อรักษาความถูกต้องของ "ราคา ณ เวลาขายจริง"
- **สินค้า inactive** ถือเป็นปัญหาคุณภาพข้อมูล ไม่ใช่แค่ FK ที่ใช้ได้ — ป้องกันไม่ให้ยอดขายของสินค้าที่เลิกขายแล้วปนเข้าไปในรายงาน
- **`dim_date` สร้างแบบ lazy** ตามวันที่ที่พบใน order จริงเท่านั้น ไม่ pre-generate ปฏิทินล่วงหน้าทั้งปี

## 7. Reflection — เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Pipeline

ข้อมูลจากภายนอกไม่มีวันสมบูรณ์แบบ 100% เสมอ หากออกแบบ Pipeline แบบ Strict ที่หยุดทำงานทั้งระบบทันทีที่
เจอแถวผิดพลาดเพียงแถวเดียว ผลกระทบจะลามไปถึงข้อมูลอีกหลายร้อยแถวที่ถูกต้องอยู่แล้วในรอบเดียวกัน ทำให้
Dashboard ยอดขายรายวันไม่มีข้อมูลใหม่เลย ซึ่งสร้างความเสียหายทางธุรกิจมากกว่าการมีข้อมูลบางส่วนที่ยังไม่
สมบูรณ์ Availability จึงสำคัญกว่า เพราะฝ่ายวิเคราะห์ยังทำงานกับข้อมูลส่วนใหญ่ที่ผ่านการตรวจสอบแล้วได้ทันที
ขณะที่ส่วนน้อยที่มีปัญหาถูกแยกไปที่ quarantine พร้อม `reason_code` ที่ตรวจสอบย้อนกลับได้ ทำให้ทีมแก้ไขเฉพาะจุด
โดยไม่กระทบข้อมูลส่วนที่เหลือ การยอมให้ Pipeline ทำงานต่อได้แบบ graceful degradation ยังช่วยรักษา SLA และ
ความน่าเชื่อถือของระบบ downstream (Dashboard, โมเดล ML) ไม่ให้ถูกกระทบเป็นวงกว้างจากปัญหาข้อมูลเล็ก ๆ
ที่เกิดขึ้นเป็นประจำ ท้ายที่สุดแล้ว Strictness ที่มากเกินไปมักเปลี่ยน "data quality issue" เล็ก ๆ ให้กลายเป็น
"system outage" ซึ่งมีต้นทุนสูงกว่ามาก
