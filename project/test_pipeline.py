import os
import unittest
import sqlite3
from project.pipeline import main, DATA_DIR

class TestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up the testing environment before running tests.
        """
        # Ensure the data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)

        # Remove any old database files
        cls.collisions_db_path = os.path.join(DATA_DIR, "collisions.db")
        cls.population_db_path = os.path.join(DATA_DIR, "population.db")
        if os.path.exists(cls.collisions_db_path):
            os.remove(cls.collisions_db_path)
        if os.path.exists(cls.population_db_path):
            os.remove(cls.population_db_path)

        # Run the pipeline
        main()

    def test_collisions_db_creation(self):
        """
        Test that the collisions.db file is created.
        """
        self.assertTrue(
            os.path.exists(self.collisions_db_path),
            "collisions.db file was not created by the pipeline"
        )

    def test_population_db_creation(self):
        """
        Test that the population.db file is created.
        """
        self.assertTrue(
            os.path.exists(self.population_db_path),
            "population.db file was not created by the pipeline"
        )

    def test_collisions_table_structure(self):
        """
        Test that the collisions table exists and has the expected structure.
        """
        conn = sqlite3.connect(self.collisions_db_path)
        cursor = conn.cursor()

        # Check table existence
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='collisions';")
        self.assertIsNotNone(cursor.fetchone(), "Table 'collisions' does not exist in collisions.db")

        # Check column existence
        expected_columns = [
            "borough", "on_street_name", "off_street_name", "cross_street_name",
            "total_injuries", "total_fatalities", "vehicle_type"
        ]
        cursor.execute("PRAGMA table_info(collisions);")
        columns = [info[1] for info in cursor.fetchall()]
        for column in expected_columns:
            self.assertIn(column, columns, f"Missing column '{column}' in 'collisions' table")

        conn.close()

    def test_population_table_structure(self):
        """
        Test that the population table exists and has the expected structure.
        """
        conn = sqlite3.connect(self.population_db_path)
        cursor = conn.cursor()

        # Check table existence
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='population';")
        self.assertIsNotNone(cursor.fetchone(), "Table 'population' does not exist in population.db")

        # Check column existence
        expected_columns = ["borough", "total_population"]
        cursor.execute("PRAGMA table_info(population);")
        columns = [info[1] for info in cursor.fetchall()]
        for column in expected_columns:
            self.assertIn(column, columns, f"Missing column '{column}' in 'population' table")

        conn.close()

    def test_no_unknown_boroughs(self):
        """
        Test that there are no rows with 'Unknown' boroughs in the collisions table.
        """
        conn = sqlite3.connect(self.collisions_db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM collisions WHERE borough='Unknown';")
        count = cursor.fetchone()[0]
        self.assertEqual(
            count, 0,
            f"There are {count} rows with 'Unknown' boroughs in the collisions table"
        )

        conn.close()

    def test_population_values(self):
        """
        Test that the total_population column has no invalid values.
        """
        conn = sqlite3.connect(self.population_db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM population WHERE total_population <= 0;")
        count = cursor.fetchone()[0]
        self.assertEqual(
            count, 0,
            f"There are {count} rows with invalid total_population values in the population table"
        )

        conn.close()

    def test_total_injuries_and_fatalities(self):
        """
        Test that the total injuries and fatalities columns are correctly populated.
        """
        conn = sqlite3.connect(self.collisions_db_path)
        cursor = conn.cursor()

        # Check for any invalid total_injuries values
        cursor.execute("SELECT COUNT(*) FROM collisions WHERE total_injuries < 0;")
        injuries_count = cursor.fetchone()[0]
        self.assertEqual(
            injuries_count, 0,
            f"There are {injuries_count} rows with invalid total_injuries in the collisions table"
        )

        # Check for any invalid total_fatalities values
        cursor.execute("SELECT COUNT(*) FROM collisions WHERE total_fatalities < 0;")
        fatalities_count = cursor.fetchone()[0]
        self.assertEqual(
            fatalities_count, 0,
            f"There are {fatalities_count} rows with invalid total_fatalities in the collisions table"
        )

        conn.close()


if __name__ == "__main__":
    unittest.main()
