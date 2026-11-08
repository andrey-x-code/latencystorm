-- Infrastructure baseline only. Domain tables are added in the corresponding MVP stage.
CREATE TABLE service_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    service_name text NOT NULL CHECK (service_name = 'results-service')
);
INSERT INTO service_identity (service_name) VALUES ('results-service');
