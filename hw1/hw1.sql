/** Question 1:  Find the number of emails that mention “Obama” in the ExtractedBodyText of the email. **/
SELECT count(*) FROM Emails WHERE ExtractedBodyText LIKE "%Obama%";
/** Question 2: Among people with Aliases, find the average number of Aliases each person has. **/
SELECT COUNT(DISTINCT Alias) / CAST(COUNT (DISTINCT(PersonId)) AS FLOAT) FROM Aliases;
/** Question 3: Find the MetadataDateSent on which the most emails were sent and the number of emails that were sent on * that date. Note that that many emails do not have a date -- don’t include those in your count. **/
SELECT * from Emails;
/** Question 4: Find out how many distinct ids refer to Hillary Clinton. Remember the hint from the homework spec! **/
CREATE VIEW numHillaryIds AS 
	SELECT P.Name, A.PersonID, count(*) FROM Aliases A, Persons P WHERE A.PersonId=P.Id AND P.Name="Hillary Clinton";
/** Question 5: Find the number of emails in the database sent by Hillary Clinton. Keep in mind that there are multiple * aliases (from the previous question) that the email could’ve been sent from. **/
-- SELECT count(*) FROM Emails E WHERE SenderPersonId=80

/** Question 6: Find the names of the 5 people who emailed Hillary Clinton the most. **/
SELECT * from Emails;
/** Question 7: Find the names of the 5 people that Hillary Clinton emailed the most. **/
SELECT * from Emails;


-- id|Aliases 			 |PersonID
-- 11|slaughter annmarie|10
-- 12|slaughter anne marie|10
-- 13|slaughter annemarie|10
-- 14|slaughtera@state.gov|10

-- SELECT AVG(Aliases), count(*) as ct GROUP BY PersonID;  

-- SELECT A.Alias, A.PersonId FROM Aliases A WHERE (SELECT count(*) FROM Alises A2 A.PersonID=A2.PersonID)

-- SELECT 
-- a.nid, a.stock, sum(b.qty)
-- FROM 
-- uc_product_stock a INNER JOIN uc_order_products b ON a.nid = b.nid
-- group by 
-- a.nid, a.stock

-- SELECT 
-- sum(A.PersonID), A.Alias
-- FROM 
-- Aliases A INNER JOIN Aliases b on A.PersonID = B.PersonId
-- group by 
-- A.PersonId, A.Alias;
wa