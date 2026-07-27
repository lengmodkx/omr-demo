"""考生信息区整体识别与解析

把一整个考生信息区（可能包含学校、班级、姓名、考场、准考证号、座号、条形码数字）
OCR 后的原始文本解析成结构化字段。
"""

import logging
import re
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

# 常见标签同义词
_NAME_LABELS = ["姓名", "名字", "考生姓名", "名"]
# 准考证号相关标签（输出到 exam_no）
_EXAM_NO_LABELS = ["准考证号", "准考证号码", "考号", "考生号"]
# 学号相关标签（输出到 student_no）
_STUDENT_NO_LABELS = ["学号", "报名号"]
_ROOM_LABELS = ["考场号", "考场", "考试场地", "试室", "考室"]
_SEAT_LABELS = ["座位号", "座号", "座位"]
_CLASS_LABELS = ["班级", "班级号"]
_SCHOOL_LABELS = ["学校", "中学", "初中", "高中"]

# 小语种/科目名称，无标签时不能当成姓名（含 OCR 常见误读：档/和/料）
_SUBJECT_WORDS = {"小语", "日语", "语文", "数学", "英语", "物理", "化学",
                  "生物", "政治", "历史", "地理", "外语", "语种", "小语种",
                  "小语档", "小语和", "小语料"}

# 小语种/科目字段标签（用于值截断与启发式清理，含 OCR 误读变体）
_MINOR_LANG_LABELS = ["小语种", "小语档", "小语和", "小语料", "语种", "科目"]

# 允许粘连取值的姓名标签（OCR 常丢失“姓名：”后的冒号，变成“姓名娜木汗”）
_GLUED_NAME_LABELS = ["考生姓名", "姓名", "名字"]


def parse_personal_info_block(raw_text: str) -> Tuple[Dict[str, str], float]:
    """解析考生信息区 OCR 原始文本。

    Returns:
        (fields, confidence)，fields 包含 name/student_no/exam_no/room/seat/class_name/school/raw_text。
    """
    logger.info("[parser-v2] 开始解析考生信息区, raw_text 长度=%s", len(raw_text) if raw_text else 0)
    if not raw_text:
        return {"raw_text": ""}, 0.0

    # 统一换行、去多余空格，保留换行用于分行
    text = raw_text.replace("\r", "\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    flat = " ".join(lines)

    fields: Dict[str, str] = {"raw_text": raw_text}

    # 1. 座号：座号/座位号 后接数字
    seat = _extract_by_label(flat, _SEAT_LABELS, r"\d+")
    if seat:
        fields["seat"] = seat

    # 2. 考场：只取标签后的连续数字，避免把粘连的“小语和：日语”吸进来
    room = _extract_by_label(flat, _ROOM_LABELS, r"\d+")
    if not room:
        room_match = re.search(r"第\s*([一二三四五六七八九十0-9]+)\s*考场", flat)
        if room_match:
            room = _cn_to_arabic(room_match.group(1))
    if room:
        fields["room"] = room

    # 3. 准考证号/学号：
    #    - 先按标签提取，区分准考证号（exam_no）和学号（student_no）；
    #    - 无标签时取全文最长数字串，考试场景默认当作准考证号；
    #    - 两者取最长，避免 "准考证号22021" 这种粘连误读覆盖真正的条码 320210011。
    nums = re.findall(r"\d{6,20}", flat)
    longest_num = max(nums, key=len) if nums else ""

    label_exam_no = _extract_by_label(flat, _EXAM_NO_LABELS, r"[A-Za-z0-9\-]{4,20}")
    label_student_no = _extract_by_label(flat, _STUDENT_NO_LABELS, r"[A-Za-z0-9\-]{4,20}")

    exam_no = label_exam_no or ""
    student_no = label_student_no or ""

    # 如果最长数字比标签提取的更长，用它补充缺失的字段
    if longest_num:
        if len(longest_num) > len(exam_no):
            exam_no = longest_num
        if len(longest_num) > len(student_no):
            student_no = longest_num

    # 只有标签明确为学号时，才把最长数字同时作为学号；否则默认仅保留 exam_no
    if not label_student_no:
        student_no = ""

    if exam_no:
        fields["exam_no"] = exam_no
    if student_no:
        fields["student_no"] = student_no

    # 4. 班级：优先按“班级”标签提取，其次匹配 X班/几年级X班
    class_name = _extract_by_label(flat, _CLASS_LABELS, r"[^\s：:]+(?:\s+[^\s：:]+)?")
    if not class_name:
        class_match = re.search(r"([一二三四五六七八九十\d]+(?:年级|年)?[（(]?[一二三四五六七八九十\d]+[）)]?\s*班)", flat)
        if class_match:
            class_name = class_match.group(1).strip()
    if not class_name:
        simple_class_match = re.search(r"(\d+)\s*班", flat)
        if simple_class_match:
            class_name = simple_class_match.group(1) + "班"
    if class_name:
        fields["class_name"] = class_name

    # 5. 学校：优先按“学校”标签提取；其次从考场标签前面提取；最后全文兜底。
    #    若提取结果包含“考场”或与考场号相同，则丢弃，避免把考场当学校。
    room_value = fields.get("room", "")
    school = _extract_by_label(flat, _SCHOOL_LABELS, r"[^\s：:]+(?:\s+[^\s：:]+)?")
    if not school:
        school = _extract_school_by_room_label(flat, room_value)
    if not school:
        school_match = re.search(
            r"((?:[^\s：:]{2,8}(?:中学|学校|初中|高中))|(?:[^\s：:]{1,8}附中)|"
            r"(?:[0-9一二三四五六七八九十]{1,8}中)|(?:[^\s：:]{2,8}中))(?:\s|$)",
            flat,
        )
        if school_match:
            school = school_match.group(1).strip()
    if school and ("考场" in school or school == room_value):
        school = ""
    if school:
        fields["school"] = school

    # 6. 姓名：
    #    优先匹配“姓名：XXX”标签
    name = _extract_by_label(flat, _NAME_LABELS, r"[^\s：:]+(?:\s+[^\s：:]+)?")
    if not name:
        # OCR 丢失冒号/空格分隔时（“姓名娜木汗”），标签后的负向预查会挡住中文名，
        # 需要单独允许姓名标签后直接粘连中文取值
        name = _extract_name_by_glued_label(flat)
    if not name:
        # 没有标签时，把已识别的字段从文本中去掉，剩下的中文里取最可能是姓名的 2-4 字词
        name = _extract_name_heuristic(flat, fields)
    if name:
        fields["name"] = name

    # 置信度：解析出的字段越多置信度越高（exam_no/student_no 任一命中即可）
    core_fields = [
        fields.get("name"),
        fields.get("exam_no") or fields.get("student_no"),
        fields.get("room"),
        fields.get("seat"),
    ]
    confidence = sum(1.0 for v in core_fields if v) / len(core_fields)

    return fields, round(confidence, 4)


def _label_pattern(label: str, value_pattern: str) -> str:
    """构造标签匹配正则。

    只限制标签后不能紧跟中文字符（避免“考场”匹配到“考场号”前缀）；
    标签前允许中文，以便处理“二连三中考场号：1”这种学校名粘连在标签前的情况。
    """
    return rf"{re.escape(label)}(?![\u4e00-\u9fa5])[：:\s]*({value_pattern})"


def _extract_by_label(text: str, labels: list, value_pattern: str = r"[^\s：:]+(?:\s+[^\s：:]+)?") -> str:
    """按“标签[:：]值”模式提取值。"""
    for label in labels:
        pattern = _label_pattern(label, value_pattern)
        m = re.search(pattern, text)
        if m:
            val = m.group(1).strip()
            # 去掉尾部常见标签/符号
            val = re.sub(r"[：:]$", "", val)
            # 若值里出现了其它字段的标签开头，则截断（避免 OCR 行连粘）
            val = _truncate_at_next_label(val, labels)
            return val
    return ""


def _truncate_at_next_label(value: str, current_labels: list) -> str:
    """如果值中出现了其它字段的标签开头，截断到该标签之前。"""
    all_labels = set(_NAME_LABELS + _STUDENT_NO_LABELS + _ROOM_LABELS + _SEAT_LABELS + _CLASS_LABELS + _SCHOOL_LABELS + _MINOR_LANG_LABELS)
    other_labels = all_labels - set(current_labels)
    for label in sorted(other_labels, key=len, reverse=True):
        idx = value.find(label)
        if idx > 0:
            return value[:idx].strip()
    return value


def _extract_name_by_glued_label(text: str) -> str:
    """处理 OCR 丢失分隔符的“姓名娜木汗”场景：姓名标签后直接粘连中文名。

    只保留取值开头的中文部分，并截断到下一个字段标签（座位号/小语种等）之前，
    避免把粘连的后续字段吸进姓名。
    """
    for label in _GLUED_NAME_LABELS:
        m = re.search(rf"{re.escape(label)}[：:\s]*([^\s：:]+)", text)
        if not m:
            continue
        val = m.group(1).strip()
        val = _truncate_at_next_label(val, _NAME_LABELS)
        # 姓名只可能是中文（含间隔号），去掉粘连的数字/字母/标点
        m2 = re.match(r"^[\u4e00-\u9fa5·]+", val)
        if m2 and len(m2.group(0)) >= 2:
            return m2.group(0)
    return ""


def _extract_name_heuristic(text: str, fields: Dict[str, str]) -> str:
    """无标签时，用启发式提取姓名。"""
    # 去掉已识别字段的文本，减少干扰
    removed = text
    for key in ("student_no", "room", "seat", "class_name", "school"):
        val = fields.get(key)
        if val:
            removed = removed.replace(val, " ")

    # 去掉纯数字串和常见标签（含小语种/科目字段，避免 OCR 误读成姓名）
    removed = re.sub(r"\b\d+\b", " ", removed)
    for label in _NAME_LABELS + _STUDENT_NO_LABELS + _ROOM_LABELS + _SEAT_LABELS + _CLASS_LABELS + _SCHOOL_LABELS + _MINOR_LANG_LABELS:
        removed = removed.replace(label, " ")

    # 按空白切分，找出纯中文 2-4 字词
    candidates = []
    for token in re.split(r"[\s：:,，]+", removed):
        token = token.strip()
        if token and 2 <= len(token) <= 4 and re.fullmatch(r"[\u4e00-\u9fa5]+", token):
            candidates.append(token)

    if not candidates:
        return ""

    # 常见学校/班级/科目后缀或整词，排除
    exclude_suffixes = ("中学", "学校", "初中", "高中", "班级", "班", "考场", "座号")
    for c in candidates:
        if c in _SUBJECT_WORDS:
            continue
        # 形如“连三中”“市一中”“附中”等学校简称，不应作为姓名
        if _looks_like_school_abbr(c):
            continue
        if not any(c.endswith(s) for s in exclude_suffixes):
            return c
    # 所有候选都不符合姓名特征，说明文本中可能没有姓名
    return ""


def _looks_like_school_abbr(token: str) -> bool:
    """判断 token 是否是学校简称（X中/附中/市一中 等）。"""
    return bool(re.fullmatch(
        r"(?:[0-9一二三四五六七八九十]+中)|(?:[^一-龥\s：:]*[一-龥]{1,6}中)|附中",
        token,
    ))


def _extract_school_by_room_label(text: str, room_value: str) -> str:
    """根据“考场/考场号”标签位置，提取其前面的学校名。

    常见格式：
      二连三中考场号：1 ...
      实验中学 考场：3 ...
    """
    if not text:
        return ""
    # 找到第一个考场标签的位置
    first_idx = -1
    matched_label = ""
    for label in _ROOM_LABELS:
        pattern = _label_pattern(label, r"")
        m = re.search(pattern, text)
        if m:
            idx = m.start()
            if first_idx == -1 or idx < first_idx:
                first_idx = idx
                matched_label = label
    if first_idx <= 0:
        return ""

    prefix = text[:first_idx].strip()
    # 从 prefix 末尾提取学校名：支持“实验中学”“第二中学”“连三中”“附中”等
    school_match = re.search(
        r"((?:[^\s：:]{2,8}(?:中学|学校|初中|高中))|(?:[^\s：:]{1,8}附中)|"
        r"(?:[0-9一二三四五六七八九十]{1,8}中)|(?:[^\s：:]{2,8}中))\s*$",
        prefix,
    )
    if school_match:
        return school_match.group(1).strip()
    return ""


def _cn_to_arabic(cn: str) -> str:
    """简单中文数字转阿拉伯数字，失败则原样返回。"""
    mapping = {
        "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
        "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
        "0": "0", "1": "1", "2": "2", "3": "3", "4": "4",
        "5": "5", "6": "6", "7": "7", "8": "8", "9": "9",
    }
    out = ""
    for ch in cn:
        out += mapping.get(ch, ch)
    digits = re.sub(r"[^0-9]", "", out)
    return digits if digits else cn
