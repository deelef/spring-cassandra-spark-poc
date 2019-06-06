CREATE KEYSPACE example WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use example;
CREATE TABLE person
(
    id         text PRIMARY KEY,
    first_name text,
    last_name  text
);


insert into example.person (id,first_name,last_name) values('1','A1','G1');
insert into example.person (id,first_name,last_name) values('2','A2','G2');
insert into example.person (id,first_name,last_name) values('3','A3','G3');
insert into example.person (id,first_name,last_name) values('4','A4','G4');
insert into example.person (id,first_name,last_name) values('5','A5','G5');
