COPY raw_zone.tmp_apartment_viewings 
        FROM 's3://nl-aws-de-labs/raw_landing_zone/apartment_db/apartment_viewings/data.csv'
        IAM_ROLE 'arn:aws:iam::127489365181:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T164456' 
        CSV
        IGNOREHEADER 1;

COPY raw_zone.apartments 
FROM 's3://nl-aws-de-labs/apartments.csv'
IAM_ROLE 'arn:aws:iam::127489365181:role/redshift-custom-role' 
CSV
IGNOREHEADER 1;

MERGE INTO raw_zone.apartment_viewings 
USING raw_zone.tmp_apartment_viewings  AS source
ON 
(
  raw_zone.apartment_viewings.apartment_id = source.apartment_id 
  and 
  raw_zone.apartment_viewings.user_id = source.user_id and raw_zone.apartment_viewings.viewed_at = source.viewed_at 
)
WHEN MATCHED THEN
    UPDATE SET
        is_wishlisted = source.is_wishlisted,
        call_to_action = source.call_to_action
WHEN NOT MATCHED THEN
    INSERT (user_id,apartment_id,viewed_at,is_wishlisted,call_to_action)
    VALUES (source.user_id,source.apartment_id,source.viewed_at,source.is_wishlisted,source.call_to_action);


COPY raw_zone.tmp_apartment_attributes 
FROM 's3://nl-aws-de-labs/raw_landing_zone/apartment_db/apartment_attributes/'
IAM_ROLE 'arn:aws:iam::127489365181:role/redshift-custom-role' 
CSV
IGNOREHEADER 1;

MERGE INTO raw_zone.apartment_attributes 
USING raw_zone.tmp_apartment_attributes  AS source
ON 
(
  raw_zone.apartment_attributes.id = source.id
)
WHEN MATCHED THEN
    UPDATE SET
        category = source.category,
        bathrooms = source.bathrooms,
        bedrooms = source.bedrooms,
        fee = source.fee,
        has_photo = source.has_photo,
        pets_allowed = source.pets_allowed,
        price_display = source.price_display,
        price_type = source.price_type,
        square_feet = source.square_feet,
        address = source.address,
        state = source.state,
        cityname = source.cityname,
        latitude=source.latitude,
        longitude = source.longitude
WHEN NOT MATCHED THEN
    INSERT (id,category,amenities,bathrooms,bedrooms,fee,has_photo,pets_allowed,price_display,price_type,square_feet,address,cityname,state,latitude,longitude)
    VALUES 
    (
      source.id,
      source.category,
      source.amenities,
      source.bathrooms,
      source.bedrooms,
      source.fee,
      source.has_photo,
      source.pets_allowed,
      source.price_display,
      source.price_type,
      source.square_feet,
      source.address,
      source.cityname,
      source.state,
      source.latitude,
      source.longitude
    );
