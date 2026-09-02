#### **user.csv**

* **id**: User ID (Primary Key)
* **name**: User name
* **email**: Email address
* **age**: Age (nullable)
* **department\_id**: Department ID (FK → `department.csv`)

#### **department.csv**

* **id**: Department ID (Primary Key)
* **name**: Department name
* **location**: Location (nullable)

#### **orders.csv**

* **id**: Order ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **amount**: Order amount
* **status**: Order status (`pending`, `shipped`, `delivered`, nullable)
* **created\_at**: Creation timestamp

#### **address.csv**

* **id**: Address ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **address**: Address (nullable)
* **postal\_code**: Postal code (nullable)

#### **salary.csv**

* **id**: Salary ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **base\_salary**: Base salary
* **bonus**: Bonus (nullable)

#### **project.csv**

* **id**: Project ID (Primary Key)
* **name**: Project name
* **department\_id**: Department ID (FK → `department.csv`)
* **budget**: Project budget (nullable)

#### **user\_project.csv**

* **id**: Relation ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **project\_id**: Project ID (FK → `project.csv`)
* **role**: Role in project (`manager`, `developer`, `tester`, nullable)

#### **attendance.csv**

* **id**: Attendance record ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **date**: Attendance date
* **status**: Attendance status (`present`, `absent`, `remote`, nullable)

#### **performance.csv**

* **id**: Performance record ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **year**: Year of evaluation
* **rating**: Performance rating (1–5, nullable)

#### **training.csv**

* **id**: Training ID (Primary Key)
* **title**: Training title
* **department\_id**: Department ID (FK → `department.csv`)
* **duration\_days**: Training duration in days

#### **user\_training.csv**

* **id**: Relation ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **training\_id**: Training ID (FK → `training.csv`)
* **completed**: Completion status (true/false, nullable)

#### **benefits.csv**

* **id**: Benefits record ID (Primary Key)
* **user\_id**: User ID (FK → `user.csv`)
* **health\_insurance**: Health insurance enrollment (true/false, nullable)
* **pension\_plan**: Pension plan enrollment (true/false, nullable)
