import psycopg2
from pymongo import MongoClient

def connect_postgresql():
    connection = psycopg2.connect(
        user="admin",
        password="admin",
        host="127.0.0.1",
        port="5432",
        database="bda_assgn_1"
    )
    cursor = connection.cursor()
    print("Connected to PostgreSQL")
    return connection, cursor

def connect_mongodb():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.bda_assgn_1
    return db

def transfer_student_data(pg_cursor, mongo_db):
    pg_cursor.execute("SELECT * FROM student")
    students = pg_cursor.fetchall()
    for student in students:
        student_data = {
            'roll_no': student[0],
            'name': student[1],
            'dept_id': student[2],
        }
        mongo_db.students.insert_one(student_data)

def transfer_instructor_data(pg_cursor, mongo_db):
    pg_cursor.execute("SELECT * FROM instructor")
    instructors = pg_cursor.fetchall()
    for instructor in instructors:
        instructor_data = {
            'id': instructor[0],
            'name': instructor[1],
            'dept_id': instructor[2],
        }
        mongo_db.instructors.insert_one(instructor_data)

def fetch_course_enrollments(pg_cursor, course_code):
    pg_cursor.execute(f"SELECT * FROM enrollment WHERE course_id = '{course_code}'")
    res = pg_cursor.fetchall()
    enrollments = list()
    for row in res:
        enrollment = {
            'roll_no': row[0],
            # skip 'course_code': row[1],
            'semester': row[2],
        }
        enrollments.append(enrollment)
    return enrollments

def update_professor_document_with_course(mongo_db, course_code, semester, professor_id):
    professor = mongo_db.instructors.find_one({'id': professor_id})
    if professor is None:
        return
    professor['courses'] = professor.get('courses', list())
    professor['courses'].append({
        'code': course_code,
        'semester': semester,
    })
    mongo_db.instructors.update_one({'id': professor_id}, {'$set': professor})

def transfer_course_data(pg_cursor, mongo_db):
    pg_cursor.execute(f"SELECT * FROM course")
    res = pg_cursor.fetchall()
    courses = list()
    course_codes = set() # To avoid duplicate courses
    for row in res:
        update_professor_document_with_course(mongo_db, row[0], row[4], row[3])
        if row[0] in course_codes:
            continue
        course_codes.add(row[0])
        enrollments = fetch_course_enrollments(pg_cursor, row[0])
        course = {
            'code': row[0],
            'name': row[1],
            'dept_id': row[2], 
            'is_core': row[5],
            'enrollments': enrollments,
        }
        courses.append(course)
    return courses

def transfer_department_data(pg_cursor, mongo_db):
    pg_cursor.execute("SELECT * FROM department")
    departments = pg_cursor.fetchall()
    for department in departments:
        dept_id = department[0]
        dept_name = department[1]
        dept_data = {
            'id': dept_id,
            'name': dept_name,
        }
        mongo_db.departments.insert_one(dept_data)

def main():
    # Connect to PostgreSQL
    pg_connection, pg_cursor = connect_postgresql()
    # Connect to MongoDB
    mongo_db = connect_mongodb()
    # Create outer collections in MongoDB
    mongo_db.create_collection('students')
    mongo_db.create_collection('instructors')
    mongo_db.create_collection('courses')
    mongo_db.create_collection('departments')
    # Transfer data from PostgreSQL to MongoDB
    transfer_department_data(pg_cursor, mongo_db)
    transfer_student_data(pg_cursor, mongo_db)
    transfer_instructor_data(pg_cursor, mongo_db)
    transfer_course_data(pg_cursor, mongo_db)
    # Close the connections
    pg_cursor.close()
    pg_connection.close()
    mongo_db.client.close()
    

if __name__ == '__main__':
    main()