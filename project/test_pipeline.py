import os
import sqlite3
import unittest
from pipeline import main, DATA_DIR


class TestPipeline(unittest.TestCase):
    def setUp(self):
        """
        Prepare the environment for testing by ensuring a clean state.
        """
        # Remove old test files
        if os.path.exists(os.path.join(DATA_DIR, "collisions.db")):
            os.remove(os.path.join(DATA_DIR, "collisions.db"))
        if os.path.exists(os.path.join(DATA_DIR, "population.db")):
            os.remove(os.path.join(DATA_DIR, "population.db"))

    def test_pipeline_execution(self):
        """
        Test that the pipeline executes without errors and creates the expected output files.
        """
        main()  # Run the pipeline

        # Assert that the output files were created
        collisions_path = os.path.join(DATA_DIR, "collisions.db")
        population_path = os.path.join(DATA_DIR, "population.db")
        self.assertTrue(os.path.exists(collisions_path), "collisions.db was not created")
        self.assertTrue(os.path.exists(population_path), "population.db was not created")

    def test_collisions_data(self):
        """
        Validate the contents of collisions.db.
        """
        collisions_path = os.path.join(DATA_DIR, "collisions.db")
        conn = sqlite3.connect(collisions_path)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='collisions';")
        self.assertIsNotNone(cursor.fetchone(), "Collisions table does not exist in collisions.db")

        # Check if there are no 'Unknown' boroughs
        cursor.execute("SELECT COUNT(*) FROM collisions WHERE borough='Unknown';")
        unknown_count = cursor.fetchone()[0]
        self.assertEqual(unknown_count, 0, "There are rows with 'Unknown' boroughs in collisions.db")

        conn.close()

    def test_population_data(self):
        """
        Validate the contents of population.db.
        """
        population_path = os.path.join(DATA_DIR, "population.db")
        conn = sqlite3.connect(population_path)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='population';")
        self.assertIsNotNone(cursor.fetchone(), "Population table does not exist in population.db")

        # Check if total_population column has valid values
        cursor.execute("SELECT COUNT(*) FROM population WHERE total_population <= 0;")
        invalid_population_count = cursor.fetchone()[0]
        self.assertEqual(invalid_population_count, 0, "There are invalid total_population values in population.db")

        conn.close()


if __name__ == "__main__":
    unittest.main()
