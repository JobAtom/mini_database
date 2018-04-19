
CREATE TABLE person (personid INT, name CHAR(40), transstatus CHAR(20), PRIMARY KEY (personid) );
CREATE TABLE account (accountid INT, personid INT, balance INT, PRIMARY KEY (accountid) );

INSERT INTO person VALUES ( 1 , 'aobo', '');
INSERT INTO account VALUES ( 1 , 1, 100);

INSERT INTO person VALUES ( 2 , 'AJ', '');
INSERT INTO account VALUES ( 2 , 2, 100);
INSERT INTO account VALUES ( 3 , 1, 10000);


BEGIN TRANSACTION
UPDATE person
SET transstatus='start'
WHERE personid=1;
SELECT accountid FROM account WHERE personid=1;

UPDATE account
SET balance=balance-1
WHERE accountid=1;
UPDATE person
SET transstatus='end'
WHERE personid=1;
COMMIT;
END TRANSACTION;


BEGIN TRANSACTION
INSERT INTO person VALUES(1,'Person 1','insert');
UPDATE person
SET transstatus='start'
WHERE personid=1;
SELECT accountid FROM account WHERE personid=1;
UPDATE account
SET balance=balance-1
WHERE accountid=100;
UPDATE person
SET transstatus='end'
WHERE personid=1;
COMMIT;
END TRANSACTION;
