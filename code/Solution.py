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
                     "id INTEGER PRIMARY KEY NOT NULL CHECK(id > 0),"
                     " description TEXT NOT NULL,"
                     " size INTEGER NOT NULL CHECK (size >= 0));"
                     "CREATE TABLE Disk("
                     "id INTEGER PRIMARY KEY NOT NULL CHECK(id > 0),"
                     "manufacturing_company TEXT NOT NULL ,"
                     " speed INTEGER NOT NULL CHECK(speed > 0),"
                     " free_space INTEGER NOT NULL CHECK ( free_space >= 0 ),"
                     " cost_per_byte INTEGER NOT NULL CHECK(cost_per_byte > 0));"
                     "CREATE TABLE RAM("
                     "id INTEGER PRIMARY KEY NOT NULL CHECK(id > 0),"
                     " company TEXT NOT NULL,"
                     " size INTEGER NOT NULL CHECK(size > 0));"
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
                     "UNIQUE (ram_id, disk_id));"
                     "CREATE VIEW Disk_And_Sum_Photos_On_Them AS "
                     "SELECT Sum_Photos_Fit_On_Disk.disk_id AS disk_id, Sum_Photos_Fit_On_Disk.speed AS speed, SUM(Sum_Photos_Fit_On_Disk.photo_id) AS sum_photos "
                     "FROM ("
                     "(SELECT id, speed FROM Disk) AS D "
                     "FULL JOIN (SELECT D1.id as disk_id, P1.id as photo_id "
                     "FROM Disk D1, Photos P1 "
                     "WHERE D1.free_space - P1.size >= 0) AS Photo_Fit_On_Disk ON D.id = Photo_Fit_On_Disk.disk_id) "
                     "AS Sum_Photos_Fit_On_Disk "
                     "GROUP BY Sum_Photos_Fit_On_Disk.disk_id, Sum_Photos_Fit_On_Disk.speed;")
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
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM Photos;"
                     "DELETE FROM Disk;"
                     "DELETE FROM RAM;"
                     "DELETE FROM Photos_In_Disk;"
                     "DELETE FROM Ram_In_Disk;"
                     "COMMIT;"
                     )
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()

def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP VIEW Disk_And_Sum_Photos_On_Them;"
                     "DROP TABLE IF EXISTS Photos CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS RAM CASCADE;"
                     "DROP TABLE IF EXISTS Photos_In_Disk CASCADE;"
                     "DROP TABLE IF EXISTS Ram_In_Disk CASCADE;"
                     "COMMIT;")
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
        conn.close()


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    photo_id = photo.getPhotoID()
    photo_description = photo.getDescription()
    photo_size = photo.getSize()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Photos(id, description,size) VALUES({id}, {description}, {size});").format(id=sql.Literal(photo_id),
                                                                                       description=sql.Literal(photo_description), size=sql.Literal(photo_size))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
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
        values = list(result.__getitem__(0).values())
        photo = Photo(*values)
        return photo


def deletePhoto(photo: Photo) -> ReturnValue:
    #Note: do not forget to adjust the free space on the disk if the photo is saved on one.!!!!!!!!!!!!!!!!!!!!!!!!!!!
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE Disk SET free_space = free_space + {photoSize} WHERE id IN (SELECT disk_id FROM Photos_In_Disk WHERE photo_id = {photoID}); "
                        "DELETE FROM Photos WHERE id={photoID};"
                        .format(photoID=photo.getPhotoID(), photoSize=photo.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


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
            "INSERT INTO Disk(id, manufacturing_company, speed, free_space, cost_per_byte)"
            "VALUES ({id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});").format(
            id=sql.Literal(disk_id), manufacturing_company=sql.Literal(disk_manufacture_company),
            speed=sql.Literal(disk_speed), free_space=sql.Literal(disk_free_space),
            cost_per_byte=sql.Literal(disk_cost_per_byte))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
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

def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected = 0
    result = Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disk WHERE id={diskID};".format(diskID=diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
        values = list(result.__getitem__(0).values())
        disk = Disk(*values)
        return disk
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



def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE id={diskID}".format(diskID=diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
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
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    ram_id = ram.getRamID()
    ram_company = ram.getCompany()
    ram_size = ram.getSize()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(id, size, company) VALUES({ram_id},{ram_size},{ram_company})").format(
            ram_id=sql.Literal(ram_id),
            ram_size=sql.Literal(ram_size),
            ram_company=sql.Literal(ram_company))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
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

def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM RAM WHERE id={ramID}")
        conn.commit()
        values = list(result.__getitem__(0).values())
        ram = RAM(*values)
        return ram
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

def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM WHERE id={ramID}".format(ramID=ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

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
            "INSERT INTO Photos(id, description, size) VALUES ({photo_id}, {description}, {size});").format(id=sql.Literal(disk_id), manufacturing_company=sql.Literal(disk_manufacture_company),
                    speed=sql.Literal(disk_speed), free_space=sql.Literal(disk_free_space),
                    cost_per_byte=sql.Literal(disk_cost_per_byte), photo_id=sql.Literal(photo_id), photo_size=sql.Literal(photo_id),
                    description=sql.Literal(photo_description), size=sql.Literal(photo_size))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
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
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Photos_In_Disk VALUES ({photo_id}, {disk_id}); "
            "UPDATE Disk SET free_space = free_space - {photo_size} WHERE id = {disk_id};"\
            .format(photo_id=photo.getPhotoID(), disk_id=diskID,
                    photo_size=photo.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
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
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "UPDATE Disk SET free_space = free_space + {photo_size} WHERE id = {disk_id} AND EXISTS(SELECT * FROM Photos_In_Disk WHERE photo_id = {photo_id} AND disk_id={disk_id}); "
            "DELETE FROM Photos_In_Disk WHERE photo_id = {photo_id} AND disk_id = {disk_id}; "
            .format(photo_id=photo.getPhotoID(), disk_id=diskID, photo_size=photo.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return ReturnValue.OK
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Ram_In_Disk VALUES ({ram_id}, {disk_id});") \
            .format(ram_id=sql.Literal(ramID), disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Ram_In_Disk WHERE ram_id = {ram_id} AND disk_id = {disk_id};".format(ram_id=ramID, disk_id=diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

def averagePhotosSizeOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT AVG(size) AS average FROM Photos INNER JOIN (SELECT photo_id FROM Photos_In_Disk WHERE disk_id = {disk_id}) AS Rel_Photos ON id = photo_id;".format(disk_id=diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
        average = list(result.__getitem__(0).values())[0]
        if average is None:
            return 0
        return average
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()

def getTotalRamOnDisk(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM(size) as sum_total_ram FROM RAM INNER JOIN (SELECT ram_id from Ram_In_Disk WHERE disk_id = {disk_id}) AS Rel_Rams ON RAM.id = Rel_Rams.ram_id;".format(disk_id=diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
        sum = list(result.__getitem__(0).values())[0]
        if sum is None:
            return 0
        return sum
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return 0
    except Exception as e:
        return -1
    finally:
        conn.close()

def getCostForDescription(description: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM(size * cost_per_byte) AS total_cost "
            "FROM ((SELECT id, size FROM Photos WHERE description = '{description}') AS P "
            "INNER JOIN Photos_In_Disk AS PID ON P.id = PID.photo_id) AS DP "
            "INNER JOIN Disk AS D ON D.id = DP.disk_id;".format(description=description))
        rows_effected, result = conn.execute(query)
        conn.commit()
        total_cost = list(result.__getitem__(0).values())[0]
        if total_cost is None:
            return 0
        return total_cost
    except Exception as e:
        return -1
    finally:
        conn.close()
def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT P.id "
            "FROM Photos AS P, Disk AS D " 
            "WHERE D.id = {diskID} AND (D.free_space - P.size >= 0) " 
            "ORDER BY P.id DESC "
            "LIMIT 5;".format(diskID=diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
        for i in range(rows_effected):
            list_to_return += result.__getitem__(i).values()
        return list_to_return
    except Exception as e:
        return []
    finally:
        conn.close()

def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT P.id "
            "FROM Photos AS P, Disk AS D "
            "WHERE (D.id = {diskID} AND (D.free_space - P.size >= 0) "
            "AND P.size <= (SELECT COALESCE(SUM(size),0) FROM RAM INNER JOIN (SELECT ram_id FROM Ram_In_Disk WHERE disk_id = {diskID}) AS Rel_Rams ON RAM.id = Rel_Rams.ram_id)) "
            "ORDER BY P.id ASC "
            "LIMIT 5;".format(diskID=diskID))
        # NEED TO SEE IF WE NEED TO ADD COALESCE
        rows_effected, result = conn.execute(query)
        conn.commit()
        for i in range(rows_effected):
            list_to_return += result.__getitem__(i).values()
        return list_to_return
    except Exception as e:
        return []
    finally:
        conn.close()

def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT COUNT(id) "
            "FROM RAM "
            "WHERE (company != ALL ((SELECT manufacturing_company FROM"
            " ((SELECT * FROM Disk WHERE id = {diskID}) AS D1 "
            "INNER JOIN (SELECT * FROM Ram_In_Disk WHERE disk_id = {diskID}) AS ROD "
            "ON D1.id = ROD.disk_id))));".format(diskID=diskID))
        # NOT SURE ABOUT IT. NEED TO CHECK
        rows_effected, result = conn.execute(query)
        conn.commit()
        ids_sum =list(result.__getitem__(0).values())[0]
        if ids_sum is None:
            return False
        if ids_sum == 0:
            return True
        return False
    except Exception as e:
        return False
    finally:
        conn.close()


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT COUNT(id) "
            "FROM Photos WHERE (description = {description}) INNER JOIN (SELECT * from Photos_In_Disk) "
            "ON id = photo_id "
            "GROUP BY disk_id "
            "HAVING COUNT(*) < {num}").format(description=description, num=num)
        rows_effected, result = conn.execute(query)
        conn.commit()
        sum =list(result.__getitem__(0).values())[0]
        if sum > 0:
            return True
        return False
    except Exception as e:
        return False
    finally:
        conn.close()

def getDisksContainingTheMostData() -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT disk_id "
            "FROM("
            "SELECT disk_id, SUM(size) AS total_data "
            "FROM Photos INNER JOIN (SELECT * from Photos_In_Disk) "
            "ON id = photo_id "
            "GROUP BY disk_id "
            "ORDER BY total_data ASC, disk_id ASC "
            "LIMIT 5)")
        rows_effected, result = conn.execute(query)
        conn.commit()
        for i in range(rows_effected):
            list_to_return += result.__getitem__(i).values()
        return list_to_return
    except Exception as e:
        return []
    finally:
        conn.close()

def getConflictingDisks() -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT D1.disk_id "
                        "FROM Photos_In_Disk as D1, Photos_In_Disk as D2 "
                        "WHERE D1.disk_id <> D2.disk_id "
                        "AND D1.photo_id = D2.photo_id "
                        "ORDER BY D1.disk_id ASC;")
        rows_effected, result = conn.execute(query)
        conn.commit()
        for i in range(rows_effected):
            list_to_return += result.__getitem__(i).values()
        return list_to_return
    except Exception as e:
        return []
    finally:
        conn.close()

def mostAvailableDisks() -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT QR.disk_id "
                        "FROM ("
                        "SELECT * Disk_And_Sum_Photos_On_Them "
                        "ORDER BY sum_photos DESC, speed DESC, disk_id ASC "
                        "LIMIT 5 )"
                        "AS QR")
        rows_effected, result = conn.execute(query)
        conn.commit()
        for i in range(rows_effected):
            list_to_return += result.__getitem__(i).values()
        return list_to_return
    except Exception as e:
        return []
    finally:
        conn.close()


def getClosePhotos(photoID: int) -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT All_Photos_And_Sum_of_Disks.photo_id "
                        "FROM "
                        "(SELECT photo_id, COUNT(disk_id) AS Count_disks FROM Photos_In_Disk"
                        " WHERE( photo_id != {photo_id}"
                        " AND disk_id IN "
                        "(SELECT disk_id FROM Photos_In_Disk WHERE Photos_In_Disk.photo_id = {photo_id}) "
                        "GROUP BY photo_id )"
                        "UNION "
                        "(SELECT id, 0 FROM Photos WHERE id != {photo_id})) AS All_Photos_And_Sum_of_Disks "
                        "WHERE (SELECT COUNT(Photos_In_Disk.disk_id) FROM Photos_In_Disk WHERE photo_id = {photo_id}) <= 2 * All_Photos_And_Sum_of_Disks.Count_disks "
                        "ORDER BY All_Photos_And_Sum_of_Disks.photo_id ASC "
                        "LIMIT 10".format(photo_id=photoID))
        rows_effected, result = conn.execute(query)
        conn.commit()
        for i in range(rows_effected):
            list_to_return += result.__getitem__(i).values()
        return list_to_return
    except Exception as e:
        return []
    finally:
        conn.close()

if __name__ == '__main__':
    clearTables()
    dropTables()
    createTables()
    print("0", averagePhotosSizeOnDisk(1))
    addDisk(Disk(1, "DELL", 10, 15, 10))
    addDisk(Disk(1,"NOA", 10, 10, 10))
    disk = getDiskByID(1)
    print("15", disk.getFreeSpace())
    print("0", averagePhotosSizeOnDisk(1))
    print(addPhoto(Photo(1, "stuff", 3)))
    print("0", averagePhotosSizeOnDisk(1))
    print(addPhotoToDisk(Photo(1, "stuff", 3), 1))
    disk = getDiskByID(1)
    print("12", disk.getFreeSpace())
    print("3", averagePhotosSizeOnDisk(1))
    print("0", averagePhotosSizeOnDisk(2))
    print("3", averagePhotosSizeOnDisk(1))
    addPhoto(Photo(2, "stuff", 5))
    print(addPhotoToDisk(Photo(2, "stuff", 5), 1))
