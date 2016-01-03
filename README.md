# TinySQL-Interpreter
The interpreter include the following components:

• A parser: the parser accepts the input Tiny-SQL query and converts it into a parse tree.

• A logical query plan generator: the logical query plan generator converts a parse tree into a logical query plan. 
This phase also includes any possible logical query plan optimization.

• A physical query plan generator: this generator converts an optimized logical query plan into an executable physical 
query plan. This phase also includes any possible physical query plan optimization.

• A set of subroutines that implement a variety of data query operations necessary for execution of queries in Tiny-SQL. 
The subroutines should use the released library StorageManager, which simulates computer disks and memory.

• Interface: the Tiny-SQL interpreter have a single-user text-based interface. The interface accepts one Tiny-SQL statement 
at a line. In addition, the interface is able to read a file containing many Tiny-SQL statements, one statement per line, 
and be able to output the query results to a file.


