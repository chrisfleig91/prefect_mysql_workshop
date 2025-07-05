drop Table Kunde;

CREATE TABLE Kunde (
    KundenID INTEGER PRIMARY KEY,
    Vorname VARCHAR(50) NOT NULL,
    Nachname VARCHAR(50) NOT NULL,
    EMail VARCHAR(100) NOT NULL,
    Geschlecht ENUM('w', 'm', 'd'),
    Registrierungsdatum DATE,
    Aktiv BOOLEAN,
    PLZ VARCHAR(20),
    Ort VARCHAR(50),
    Strasse VARCHAR(50),
    Hausnummer VARCHAR(20),
    Land VARCHAR(60),
    Firma VARCHAR(100),
    Quellsystem VARCHAR(30)
);