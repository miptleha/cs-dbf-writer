DbfWriter
=========

Raw creation of dbf files. Files saved in dBASE III 2.0 Format.

There is test application that shows how to use library and compares speed of
raw writing and writing via Microsoft driver.

Content of project
------------------

-   DbfWriter - Library with classes DbfWriterFast (direct to file writing) and
    DbfWriterMicrosoft (using oledb driver).

-   Test - Console program that generates test data and compares time of
    execution writing methods of each class.

How to run
----------

Open DbfWriter.sln in Visual Studio, set Test as Startup project and run. In
Program.cs can adjust size of test data.
