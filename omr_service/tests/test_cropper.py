"""主观题裁剪模块单元测试"""
import os
import tempfile
import unittest

import cv2
import numpy as np

from omr_service.engine.cropper import SubjectiveCropper


class TestSubjectiveCropper(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cropper = SubjectiveCropper(output_dir=self.temp_dir)
        # 构造两页不同尺寸的图片
        self.page0 = np.full((200, 300, 3), 255, dtype=np.uint8)
        cv2.rectangle(self.page0, (50, 50), (150, 150), (0, 0, 0), -1)
        self.page1 = np.full((200, 300, 3), 255, dtype=np.uint8)
        cv2.rectangle(self.page1, (50, 50), (150, 150), (128, 128, 128), -1)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_crop_single_region(self):
        """单页单区域裁剪"""
        regions = [
            {"q": 1, "x1": 40, "y1": 40, "x2": 160, "y2": 160, "page_index": 0, "stitch_with_next": False}
        ]
        results = self.cropper.crop_subjective_regions([self.page0], regions, "ns1")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["q"], 1)
        self.assertTrue(results[0]["image_url"].startswith("file://"))
        self.assertTrue(os.path.exists(results[0]["image_url"].replace("file://", "")))

    def test_crop_with_base_url(self):
        """配置了 base_url 时返回可访问 URL"""
        cropper = SubjectiveCropper(output_dir=self.temp_dir, base_url="https://oss.example.com/crops")
        regions = [
            {"q": 1, "x1": 40, "y1": 40, "x2": 160, "y2": 160, "page_index": 0, "stitch_with_next": False}
        ]
        results = cropper.crop_subjective_regions([self.page0], regions, "ns2")
        self.assertTrue(results[0]["image_url"].startswith("https://oss.example.com/crops/"))

    def test_stitch_cross_page(self):
        """跨页拼接裁剪"""
        regions = [
            {"q": 1, "x1": 40, "y1": 40, "x2": 160, "y2": 160, "page_index": 0, "stitch_with_next": True},
            {"q": 1, "x1": 40, "y1": 40, "x2": 160, "y2": 160, "page_index": 1, "stitch_with_next": False},
        ]
        results = self.cropper.crop_subjective_regions([self.page0, self.page1], regions, "ns3")
        self.assertEqual(len(results), 1)
        # 拼接后高度约为两页之和
        saved_path = results[0]["image_url"].replace("file://", "")
        stitched = cv2.imread(saved_path)
        self.assertIsNotNone(stitched)
        self.assertGreater(stitched.shape[0], 300)

    def test_crop_by_regions(self):
        """单图按区域列表裁剪"""
        regions = [
            {"q": 2, "x1": 40, "y1": 40, "x2": 160, "y2": 160, "page_index": 0},
        ]
        results = self.cropper.crop_by_regions(self.page0, regions, "ns4")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["q"], 2)


if __name__ == "__main__":
    unittest.main()
