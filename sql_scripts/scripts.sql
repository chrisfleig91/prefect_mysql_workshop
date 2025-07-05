DELETE k
FROM Kunde k
WHERE KundenID = 100000;

ALTER TABLE Kunde
ADD COLUMN Quellsystem VARCHAR(30);

INSERT INTO Kunde (
    KundenID,
    Geschlecht,
    Vorname,
    Nachname,
    EMail,
    Registrierungsdatum,
    Land,
    Aktiv,
    Firma,
    Ort,
    PLZ,
    Strasse,
    Hausnummer
  )
VALUES (
    100000,
    'w',
    'Anna',
    'Müller',
    'anna.mueller@example.com',
    '2022-01-15',
    'Deutschland',
    TRUE,
    'Müller & Co GmbH',
    'Berlin',
    '10115',
    'Friedrichstraße',
    '123'
  );

UPDATE Kunde
SET Quellsystem = 'E-Commerce'
WHERE KundenID = 1001;

truncate table Kunde;

delete from Kunde;

drop table Kunde;