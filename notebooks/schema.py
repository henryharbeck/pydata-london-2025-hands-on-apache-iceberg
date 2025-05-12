from pyiceberg.schema import Schema, NestedField, StringType, IntegerType, DateType

house_prices_schema = Schema(
    NestedField(
        1,
        "transaction_id",
        StringType(),
        required=True,
        doc="A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.",
    ),
    NestedField(
        2, "price", IntegerType(), required=True, doc="Sale price stated on the transfer deed."
    ),
    NestedField(
        3,
        "date_of_transfer",
        DateType(),
        required=True,
        doc="Date when the sale was completed, as stated on the transfer deed.",
    ),
    NestedField(
        4,
        "postcode",
        StringType(),
        required=True,
        doc="This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.",
    ),
    NestedField(
        5,
        "property_type",
        StringType(),
        required=True,
        doc="D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other",
    ),
    NestedField(
        6,
        "new_property",
        StringType(),
        required=True,
        doc="Indicates the age of the property and applies to all price paid transactions, residential and non-residential. Y = a newly built property, N = an established residential building",
    ),
    NestedField(
        7,
        "duration",
        StringType(),
        required=True,
        doc="Relates to the tenure: F = Freehold, L= Leasehold etc. Note that HM Land Registry does not record leases of 7 years or less in the Price Paid Dataset.",
    ),
    NestedField(
        8,
        "paon",
        StringType(),
        doc="Primary Addressable Object Name. Typically the house number or name",
    ),
    NestedField(
        9,
        "saon",
        StringType(),
        doc="Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.",
    ),
    NestedField(10, "street", StringType()),
    NestedField(11, "locality", StringType()),
    NestedField(12, "town", StringType()),
    NestedField(13, "district", StringType()),
    NestedField(14, "county", StringType()),
    NestedField(
        15,
        "ppd_category_type",
        StringType(),
        doc="Indicates the type of Price Paid transaction. A = Standard Price Paid entry, includes single residential property sold for value. B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to non-private individuals and sales where the property type is classed as ‘Other’.",
    ),
    NestedField(16, "record_status", StringType(),
                doc="Indicates additions, changes and deletions to the records. A = Addition C = Change D = Delete"),
    identifier_field_ids=[1],
)