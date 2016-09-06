/** Question 1:  Find the number of emails that mention “Obama” in the ExtractedBodyText of the email. **/
SELECT COUNT(*) FROM Emails WHERE ExtractedBodyText LIKE "%Obama%";
/** Question 2: Among people with Aliases, find the average number of Aliases each person has. **/
SELECT COUNT(DISTINCT Alias) / CAST(COUNT(DISTINCT(PersonId)) AS FLOAT) FROM Aliases;
/** Question 3: Find the MetadataDateSent on which the most emails were sent and the number of emails that were sent on * that date. Note that that many emails do not have a date -- don’t include those in your count. **/
SELECT COUNT(*) as ct, MetadataDateSent FROM Emails WHERE MetadataDateSent!='' GROUP BY MetadataDateSent ORDER BY ct DESC LIMIT 1;
/** Question 4: Find out how many distinct ids refer to Hillary Clinton. Remember the hint from the homework spec! **/
CREATE VIEW NumHillaryIds AS 
	SELECT P.Name, A.PersonId, COUNT(*) as Count FROM Aliases A, Persons P WHERE A.PersonId=P.Id AND P.Name="Hillary Clinton";
SELECT N.Name, N.Count FROM NumHillaryIds N;
/** Question 5: Find the number of emails in the database sent by Hillary Clinton. Keep in mind that there are multiple * aliases (from the previous question) that the email could’ve been sent from. **/
SELECT COUNT(*) FROM Emails E, NumHillaryIds N WHERE E.SenderPersonId=N.PersonId;
/** Question 6: Find the names of the 5 people who emailed Hillary Clinton the most. **/
SELECT P.Name, COUNT(*) as count 
FROM Emails E, Aliases A, Persons P, NumHillaryIds N
WHERE A.Alias = E.MetaDataFrom and A.PersonId = P.id and P.id!=N.PersonId
GROUP BY A.PersonId 
ORDER BY -count LIMIT 5;
/** Question 7: Find the names of the 5 people that Hillary Clinton emailed the most. **/
SELECT P.Name, COUNT(*) as count 
FROM Emails E, Aliases A, Persons P
WHERE A.Alias = E.MetadataTo and A.PersonId = P.id
GROUP BY A.PersonId 
ORDER BY -count LIMIT 5;
