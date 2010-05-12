import unittest
from database import Database

class DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        
    def test_get_monitors(self):
        monitor = self.database.get_monitor('frank')
        monitors = self.database.get_monitors()
        self.assertTrue(monitor in monitors)
        
    def test_get_groups(self):
        group = self.database.get_group_by_name('quae-other')
        groups = self.database.get_groups()
        self.assertTrue(group in groups)
        
    def tearDown(self):
        self.database = None
    
if __name__ == '__main__':
    unittest.main()
