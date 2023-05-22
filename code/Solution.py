from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    pass


def clearTables():
    pass


def dropTables():
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    return Photo()


def deletePhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        disk_id = disk.getDiskID()
        disk_manufacture_company = disk.getCompany()
        disk_speed = disk.getSpeed()
        disk_free_space = disk.getFreeSpace()
        disk_cost_per_byte = disk.getCost()
        query = sql.SQL(
            "INSERT INTO Disk(id, manufacturing_company, speed, free_space, cost_per_byte) VALUES ({id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte})").format(
            id=sql.Literal(disk_id), manufacturing_company=sql.Literal(disk_manufacture_company),
            speed=sql.Literal(disk_speed), free_space=sql.Literal(disk_free_space),
            cost_per_byte=sql.Literal(disk_cost_per_byte))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    # except DatabaseException.FOREIGN_KEY_VIOLATION as e:
    # return
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected = 0
    result = Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disk WHERE id={diskID}").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute()
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        return Disk.badDisk()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Disk.badDisk()
    except DatabaseException.CHECK_VIOLATION as e:
        return Disk.badDisk()
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Disk.badDisk()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Disk.badDisk()
    except Exception as e:
        return Disk.badDisk()
    finally:
        conn.close()
        return result
        # need to know how to transform a result into Disk, photo, ETC


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE id={id}").format(sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR()
    # except DatabaseException.NOT_NULL_VIOLATION as e:
    # print(e)
    # except DatabaseException.CHECK_VIOLATION as e:
    # print(e)
    # except DatabaseException.UNIQUE_VIOLATION as e:
    # print(e)
    # except DatabaseException.FOREIGN_KEY_VIOLATION as e:
    # print(e)
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        if rows_effected == 0:
            return ReturnValue.ALREADY_EXISTS
        return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    conn = None
    disk_id = disk.getDiskID()
    disk_manufacture_company = disk.getCompany()
    disk_speed = disk.getSpeed()
    disk_free_space = disk.getFreeSpace()
    disk_cost_per_byte = disk.getCost()

    photo_id = photo.getPhotoID()
    photo_description = photo.getDescription()
    photo_size = photo.getSize()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Disk(id, manufacturing_company, speed, free_space, cost_per_byte) VALUES ({id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});"
            "INSERT INTO Photo(id, description, size) VALUES ({photo_id}, {description}, {size}) ") \
            .format(id=sql.Literal(disk_id), manufacturing_company=sql.Literal(disk_manufacture_company),
                    speed=sql.Literal(disk_speed), free_space=sql.Literal(disk_free_space),
                    cost_per_byte=sql.Literal(disk_cost_per_byte), photo_size=sql.Literal(photo_id),
                    description=photo_description, size=photo_size)
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    # except DatabaseException.FOREIGN_KEY_VIOLATION as e:
    # return
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK

def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    return 0


def getTotalRamOnDisk(diskID: int) -> int:
    return 0


def getCostForDescription(description: str) -> int:
    return 0


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description: str, num: int) -> bool:
    return True


def getDisksContainingTheMostData() -> List[int]:
    return []


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []
