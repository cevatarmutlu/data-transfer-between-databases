#### In-Python module ####
import unittest

#### Project Scripts ####
from src.Covert import convert
from src.db.DBFormatEnum import DBFormatEnum

class TestConvert(unittest.TestCase):

    class_name = "Convert"

    def test0_convert(self):

        print(f'\n**Start {TestConvert.class_name} convert() test**\n') 

        with self.assertRaises(TypeError):
            # Test source parameter
            convert('12', DBFormatEnum.DICT, [1, 2, 3])
            convert(12, DBFormatEnum.DICT, [1, 2, 3])
            convert([], DBFormatEnum.DICT, [1, 2, 3])

            # Test target parameter
            convert(DBFormatEnum.DICT, '12', [1, 2, 3])
            convert(DBFormatEnum.DICT, 12, [1, 2, 3])
            convert(DBFormatEnum.DICT, [], [1, 2, 3])

            # Test data parameter
            convert(DBFormatEnum.DICT, DBFormatEnum.TUPLE, 1)
            convert(DBFormatEnum.DICT, DBFormatEnum.TUPLE, {})
            convert(DBFormatEnum.DICT, DBFormatEnum.TUPLE, '34')

        print(f'\n**End {TestConvert.class_name} convert() test**\n') 
