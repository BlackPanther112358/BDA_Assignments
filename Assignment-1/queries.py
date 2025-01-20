from pyspark.sql import SparkSession

df_students = None
df_instructors = None
df_departments = None
df_courses = None

def connect_mongodb_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark MongoDB Example") \
        .getOrCreate()

    global df_students, df_instructors, df_departments, df_courses

    # Students collection
    spark.conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/bda_assgn_1.students")
    df_students = spark.read.format("mongo").load()

    # Instructors collection
    spark.conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/bda_assgn_1.instructors")
    df_instructors = spark.read.format("mongo").load()

    # Department collection
    spark.conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/bda_assgn_1.departments")
    df_departments = spark.read.format("mongo").load()

    # Courses collection
    spark.conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/bda_assgn_1.courses")
    df_courses = spark.read.format("mongo").load()

    return spark

def fetch_all_students_in_course(course_code):
    enrollments = df_courses.filter(df_courses['code'] == course_code).select('enrollments')
    students = list()
    for enrollment in enrollments:
        student = df_students.filter(df_students['roll_no'] == enrollment['roll_no'])
        students.append(student)
    print(students)

def avg_student_cnt_for_professor(professor_id):
    student_cnt = 0
    courses_taught = df_instructors.filter(df_instructors['id'] == professor_id).select('courses')
    course_cnt = len(courses_taught)
    for course in courses_taught:
        course_code = course['code']
        semester = course['semester']
        enrollments = df_courses.filter(df_courses['code'] == course_code).select('enrollments')
        enrollments = enrollments.filter(enrollments['semester'] == semester)
        student_cnt += len(enrollments)
    print(student_cnt / course_cnt)

def courses_offered_by_department(dept_id):
    courses_offered = df_courses.filter(df_courses['dept_id'] == dept_id).select('code', 'name')
    courses_offered.show()

def number_of_students_per_department():
    students_per_department = df_students.groupBy('dept_id').count()
    students_per_department.show()

def instructors_who_taught_all_CSE_core_courses():
    cse_core_courses = df_courses.filter(df_courses['dept_name'] == 'CSE').filter(df_courses['is_core'] == True).select('code')
    cse_core_courses = cse_core_courses.collect()
    cse_core_courses = [course['code'] for course in cse_core_courses]
    instructors = df_instructors.select('id')
    instructors = instructors.collect()
    instructors = [instructor['id'] for instructor in instructors]
    instructors_who_taught_all = list()
    for instructor in instructors:
        courses_taught = df_instructors.filter(df_instructors['id'] == instructor).select('courses')
        courses_taught = courses_taught.collect()
        courses_taught = [course['code'] for course in courses_taught]
        if set(cse_core_courses).issubset(courses_taught):
            instructors_who_taught_all.append(instructor)
    print(instructors_who_taught_all)

def courses_with_high_enrollment(ret_cnt = 10):
    top_enrolled_courses = df_courses.select('code', 'name').sort('enrollments', ascending=False).limit(ret_cnt)
    top_enrolled_courses.show()

def main():
    # Open a connection to MongoDB
    spark = connect_mongodb_spark()

    # Run the required query
    fetch_all_students_in_course('CSE101')

    # Close the connection
    spark.stop()

if __name__ == "__main__":
    main()
