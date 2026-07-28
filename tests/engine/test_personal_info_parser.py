"""考生信息区解析器单元测试。"""

import unittest

from omr_service.engine.personal_info_block_parser import parse_personal_info_block


class TestPersonalInfoBlockParser(unittest.TestCase):
    """覆盖用户反馈的典型 OCR 粘连/学校简称场景。"""

    def test_school_abbr_without_number_prefix(self):
        """学校简称首字被 OCR 漏掉时仍应识别为学校，不污染姓名。"""
        raw = "连三中考场号：2小语：日语 姓名：佟子怡 座位号：54 320220054"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("佟子怡", fields.get("name"))
        self.assertEqual("连三中", fields.get("school"))
        self.assertEqual("2", fields.get("room"))
        self.assertEqual("54", fields.get("seat"))
        self.assertEqual("320220054", fields.get("exam_no"))

    def test_school_abbr_with_number_prefix(self):
        """完整学校名（中文数字+简称）可正常识别。"""
        raw = "二连三中考场号：2小语：日语 姓名：佟子怡 座位号：54 320220054"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("佟子怡", fields.get("name"))
        self.assertEqual("二连三中", fields.get("school"))

    def test_user_case_namuhan(self):
        """用户截图中娜木汗的实际识别文本：学校简称缺少首字。"""
        raw = "连三中考场号：2小语：日语 姓名：娜木汗 座位号：65 320220065 320220065"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("娜木汗", fields.get("name"))
        self.assertEqual("连三中", fields.get("school"))
        self.assertEqual("65", fields.get("seat"))

    def test_full_school_name(self):
        """常规“XX中学”学校名仍可识别。"""
        raw = "实验中学考场号：1 姓名：张三 座位号：5 320210005"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("张三", fields.get("name"))
        self.assertEqual("实验中学", fields.get("school"))

    def test_school_with_cn_number(self):
        """“第二中学”类学校名仍可识别。"""
        raw = "第二中学考场号：1 姓名：李四 座位号：10 320210010"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("李四", fields.get("name"))
        self.assertEqual("第二中学", fields.get("school"))

    def test_school_fuzhong(self):
        """“附中”类简称可识别。"""
        raw = "师大附中 考场号：3 姓名：王五 座位号：1 320210031"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("王五", fields.get("name"))
        self.assertEqual("师大附中", fields.get("school"))

    def test_subject_words_not_name(self):
        """无姓名标签时，不能把“日语/小语”等科目名当作姓名。"""
        raw = "连三中考场号：2小语：日语 座位号：54 320220054"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("连三中", fields.get("school"))
        # 姓名缺失，不能错认成“日语”或“小语”
        self.assertNotIn(fields.get("name"), {"日语", "小语", "小语种"})

    def test_name_glued_to_label_and_subject_misread(self):
        """用户截图实际文本：冒号丢失“姓名娜木汗” + “小语种”被 OCR 误读为“小语档”。"""
        raw = "二连三中考场号： 2小语档： 日语 姓名娜木汗 座位号： 65 320220065"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("娜木汗", fields.get("name"))
        self.assertEqual("二连三中", fields.get("school"))
        self.assertEqual("2", fields.get("room"))
        self.assertEqual("65", fields.get("seat"))
        self.assertEqual("320220065", fields.get("exam_no"))

    def test_name_glued_with_following_label(self):
        """姓名与后续字段完全无空格时，应在下一个标签处截断。"""
        raw = "二连三中 考场号：2 姓名娜木汗座位号：65 320220065"
        fields, _ = parse_personal_info_block(raw)
        self.assertEqual("娜木汗", fields.get("name"))
        self.assertEqual("65", fields.get("seat"))

    def test_subject_misread_variant_not_name(self):
        """无姓名标签时，“小语种”的 OCR 误读变体也不能当作姓名。"""
        raw = "二连三中考场号：2小语档：日语 座位号：65 320220065"
        fields, _ = parse_personal_info_block(raw)
        self.assertNotIn(fields.get("name"), {"日语", "小语档", "小语和", "小语料"})


if __name__ == "__main__":
    unittest.main()
