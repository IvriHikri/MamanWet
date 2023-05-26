from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql
from Utility.DBConnector import ResultSet


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Photos("
                     "id INTEGER PRIMARY KEY CHECK(id >= 0),"
                     " description TEXT NOT NULL,"
                     " size INTEGER NOT NULL;"
                     "CREATE TABLE Disk("
                     "id INTEGER PRIMARY KEY CHECK(id > 0),"
                     "manufacturing_company TEXT NOT NULL ,"
                     " speed INTEGER NOT NULL CHECK(speed > 0),"
                     " free_space INTEGER NOT NULL,"
                     " cost_per_byte INTEGER NOT NULL CHECK(cost_per_byte > 0));"
                     "CREATE TABLE RAM("
                     "id INTEGER PRIMARY KEY CHECK(id > 0),"
                     " size INTEGER NOT NULL CHECK(size > 0),"
                     " company TEXT NOT NULL);"
                     "CREATE TABLE Photos_In_Disk("
                     "photo_id INTEGER CHECK ( photo_id > 0 ),"
                     "disk_id INTEGER CHECK ( disk_id > 0 ),"
                     "FOREIGN KEY (photo_id) REFERENCES Photos(id) ON DELETE CASCADE ,"
                     "FOREIGN KEY (disk_id) REFERENCES  Disk(id) ON DELETE CASCADE ,"
                     "UNIQUE (photo_id, disk_id));"
                     "CREATE TABLE Ram_In_Disk("
                     "ram_id INTEGER CHECK ( ram_id > 0 ),"
                     "disk_id INTEGER CHECK ( disk_id > 0 ),"
                     "FOREIGN KEY (ram_id) REFERENCES RAM (id) ON DELETE CASCADE ,"
                     "FOREIGN KEY (disk_id) REFERENCES Disk (id) ON DELETE CASCADE ,"
                     "UNIQUE (ram_id, disk_id));")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    pass


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Users CASCADE")#change!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    photo_id = photo.getPhotoID()
    photo_description = photo.getDescription()
    photo_size = photo.getSize()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Photos(id, description,size) VALUES({id}, {description}, {size})").format(id=sql.Literal(photo_id),
                                                                                       description=sql.Literal(photo_description), size = sql.Literal(photo_size))
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
    #except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        #return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK



def getPhotoByID(photoID: int) -> Photo:
    conn = None
    rows_effected,result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM Photos WHERE id={photoID}")
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        return Photo.badPhoto()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Photo.badPhoto()
    except DatabaseException.CHECK_VIOLATION as e:
        return Photo.badPhoto()
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Photo.badPhoto()
    except Exception as e:
        return Photo.badPhoto()
    finally:
        conn.close()
        values = list(result._getitem_(0).values())
        photo = Photo(*values)
        return photo


def deletePhoto(photo: Photo) -> ReturnValue:
    #Note: do not forget to adjust the free space on the disk if the photo is saved on one.!!!!!!!!!!!!!!!!!!!!!!!!!!!
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Photos WHERE id={0}").format(sql.Literal(photo.getPhotoID()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        #return rows_effected
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
            "INSERT INTO Disk(id,"
            " manufacturing_company,"
            " speed,"
            " free_space,"
            " cost_per_byte) VALUES ({id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte})").format(
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
        query = sql.SQL("SELECT * FROM Disk WHERE id={0}").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
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
        values = list(result._getitem_(0).values())
        disk = Disk(*values)
        return disk


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE id={id}").format(sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
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
    conn = None
    ram_id = ram.getRamID()
    ram_company = ram.getCompany()
    ram_size = ram.getSize()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(id, size, company) VALUES({ram_id},{ram_size},{ram_company})").format(
            id=sql.Literal(ram_id),
            size=sql.Literal(ram_size),
            company=sql.Literal(ram_company))
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
    # return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK



def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM RAM WHERE id={ramID}")
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        return RAM.badRAM()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return RAM.badRAM()
    except DatabaseException.CHECK_VIOLATION as e:
        return RAM.badRAM()
    except DatabaseException.UNIQUE_VIOLATION as e:
        return RAM.badRAM()
    except Exception as e:
        return RAM.badRAM()
    finally:
        conn.close()
        values = list(result._getitem_(0).values())
        ram = RAM(*values)
        return ram


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM WHERE id={0}").format(sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return rows_effected
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
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Photos_In_Disk VALUES ({photo_id}, {disk_id});"
            "UPDATE Disk SET free_space = free_space - {photo_size} WHERE id = {disk_id}") \
            .format(photo_id=sql.Literal(photo.getPhotoID()), disk_id=sql.Literal(diskID),
                    photo_size=sql.Literal(photo.getSize()))
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
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    finally:
        conn.close()
        return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Photos_In_Disk WHERE photo_id = {photo_id} AND disk_id {disk_id});"
            "UPDATE Disk SET free_space = free_space + {photo_size} WHERE id = {disk_id}") \
            .format(photo_id=sql.Literal(photo.getPhotoID()), disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.OK
    finally:
        conn.close()
        return ReturnValue.OK

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Ram_In_Disk VALUES ({ram_id}, {disk_id});" \
            .format(ram_id=sql.Literal(ramID), disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    finally:
        conn.close()
        return ReturnValue.OK
def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Ram_In_Disk WHERE ram_id = {ram_id} AND disk_id {disk_id});" \
            .format(ram_id=sql.Literal(ramID), disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.OK
    finally:
        conn.close()
        return ReturnValue.OK

def averagePhotosSizeOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT AVG(size) as average FROM Photo INNER JOIN (SELECT photo_id from Photos_In_Disk WHERE disk_id = {disk_id}) AS Rel_Photos ON id = photo_id;" \
                .format(disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return 0
    except Exception as e:
        return -1
    finally:
        average = list(result._getitem_(0).values())[0]
        conn.close()
        if average is None:
            return 0
        return average

def getTotalRamOnDisk(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM(size) as sum_total_ram FROM RAM INNER JOIN (SELECT ram_id from Ram_In_Disk WHERE disk_id = {disk_id}) AS Rel_Rams ON id = ram_id_id;" \
                .format(disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return 0
    except Exception as e:
        return -1
    finally:
        sum = list(result._getitem_(0).values())[0]
        conn.close()
        return sum

def getCostForDescription(description: str) -> int:
    return 0


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    return True


def getDisksContainingTheMostData() -> List[int]:
    return []


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []