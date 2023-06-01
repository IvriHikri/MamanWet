import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    def test_Disk(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 already exists")

    def test_RAM(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(3, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAM(RAM(3, "find minimum value", 10)),
                         "ID 1 already exists")

    def test_Photo(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "Tree", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "Tree", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "Tree", 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhoto(Photo(3, "Tree", 10)),
                         "ID 1 already exists")

    def test_Disk_add_get_and_remove(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "HP", 0, 10, 10)), "Speed 0 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(0, "HP", 10, 10, 10)), "ID 0 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "HP", 10, 10, 0)), "Cost 0 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "HP", 10, -1, 10)), "Free space -1 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(None, "HP", 10, -1, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, None, 10, 10, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "HP", None, 10, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "HP", 10, None, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "HP", 10, 10, None)), "NULL is not allowed")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "HP", 10, 0, 10)), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(1, "HP", 0, 10, 10)),
                         "BAD_PARAMS has precedence over ALREADY_EXISTS")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(1, None, 10, 10, 10)),
                         "BAD_PARAMS has precedence over ALREADY_EXISTS")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getDiskID(), 2, "Should work")
        self.assertEqual(disk.getCompany(), "DELL", "Should work")
        self.assertEqual(disk.getSpeed(), 10, "Should work")
        self.assertEqual(disk.getCost(), 10, "Should work")
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(4), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteDisk(4), "ID 4 was already removed")
        Solution.clearTables()
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteDisk(1), "Tables should be empty")
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addDisk(Disk(1,"HP",1,1,1)), "Should error")
        self.assertEqual(ReturnValue.ERROR, Solution.deleteDisk(1), "Should error")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), None, "Should return badDisk")
        self.assertEqual(disk.getCompany(), None, "Should return badDisk")
        self.assertEqual(disk.getSpeed(), None, "Should return badDisk")
        self.assertEqual(disk.getCost(), None, "Should return badDisk")
        self.assertEqual(disk.getFreeSpace(), None, "Should return badDisk")
        Solution.createTables()
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK,Solution.deleteDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "HP", 5, 5, 5)), "Re-adding disk 1")


if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
