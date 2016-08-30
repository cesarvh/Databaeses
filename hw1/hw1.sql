/** Question 1:  Find the number of emails that mention “Obama” in the ExtractedBodyText of the email. **/
SELECT count(*) FROM Emails WHERE ExtractedBodyText LIKE "%Obama%";
/** Question 2: Find the average number of aliases each people in the `Persons` table has. **/
SELECT AVG(A.Id=P.Id) FROM Persons as P, Aliases as A;
/** Question 3: Find the MetadataDateSent on which the most emails were sent and the number of emails that were sent on * that date. Note that that many emails do not have a date -- don’t include those in your count. **/
SELECT * from Emails;
/** Question 4: Find out how many distinct ids refer to Hillary Clinton. Remember the hint from the homework spec! **/
SELECT * from Emails;
/** Question 5: Find the number of emails in the database sent by Hillary Clinton. Keep in mind that there are multiple * aliases (from the previous question) that the email could’ve been sent from. **/
SELECT * from Emails;
/** Question 6: Find the names of the 5 people who emailed Hillary Clinton the most. **/
SELECT * from Emails;
/** Question 7: Find the names of the 5 people that Hillary Clinton emailed the most. **/
SELECT * from Emails;
