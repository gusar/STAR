import unittest

from star.utils import flatten_json


class TestFlattenJson(unittest.TestCase):
    def test_flatten_json_one_level(self):
        test_dic = {'a': '1', 'b': '2', 'c': 3}

        output_dict_size = len(flatten_json(test_dic))

        self.assertEqual(output_dict_size, 3)

    def test_one_flatten(self):
        test_dic = {'a': '1',
                    'b': '2',
                    'c': {'c1': '3', 'c2': '4'}}

        output_dict_size = len(flatten_json(test_dic))

        self.assertEqual(output_dict_size, 4)

    def test_one_flatten_keys(self):
        test_dic = {'a': '1',
               'b': '2',
               'c': {'c1': '3', 'c2': '4'}}

        output_keys = flatten_json(test_dic).keys()
        expected_keys = ['a', 'b', 'c_c1', 'c_c2']

        self.assertSetEqual(set(output_keys), set(expected_keys))

    def test_three_flatten(self):
        test_dic = {
            'a': 1,
            'b': 2,
            'c': [{'d': [2, 3, 4], 'e': [{'f': 1, 'g': 2}]}]}

        expected_keys = ['a', 'b', 'c_0_e_0_g', 'c_0_e_0_f', 'c_0_d_1', 'c_0_d_0', 'c_0_d_2']
        flattened = flatten_json(test_dic)

        self.assertSetEqual(set(expected_keys), set(flattened.keys()))


if __name__ == '__main__':
    unittest.main()
