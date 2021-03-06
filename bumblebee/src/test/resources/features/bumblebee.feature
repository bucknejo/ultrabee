@bumblebee-integration-test
Feature: Bumblebee Integration Test
  Test for ETL - Extract (read JSON), Transform (Transform DataFrames), Load (Dump data to persistence)

  Background: Mechanism to execute steps before all scenarios
    Given: Set environment variables
  Scenario: Run Bumblebee
    When Run Bumblebee for 20171221
    Then Compare DataFrame MANUFACTURERS from EXTRACT order by manufacturer_id
      | id:int | guid:string                          | is_active:boolean | name:string                    | manufacturer_id:string | description:string                                                                               | is_retailer:boolean | is_wholesaler:boolean | location:string                                                                                                            |
      | 247    | 592f35ae-a541-42ba-a4ad-ad8831202636 | true              | Willms-O'Conner                | AAHH-KQ6566-2311       | dapibus augue vel accumsan tellus nisi eu orci mauris lacinia sapien quis                        | false               | true                  | {"location":{"address1":"0539 Victoria Terrace","city":"Topeka","country":"US","state":"Kansas","zip":"66699"}}            |
      | 199    | 2aa6368b-59a4-4782-8bab-59a2c3378f1d | false             | Ebert Group                    | ACPI-BM5744-4442       | erat fermentum justo nec condimentum neque sapien placerat ante nulla justo aliquam quis         | true                | false                 | {"location":{"address1":"574 Oak Valley Drive","city":"Phoenix","country":"US","state":"Arizona","zip":"85072"}}           |
      | 107    | 3bbbd989-a9af-46e5-8453-eee1ea4ac664 | true              | Bauch-Bergnaum                 | ADNE-QL4757-5275       | donec ut mauris eget massa tempor convallis nulla neque libero convallis                         | false               | false                 | {"location":{"address1":"5258 Nancy Way","city":"Saint Augustine","country":"US","state":"Florida","zip":"32092"}}         |
      | 270    | 38c49062-3b25-407d-b1b1-74f5db46a837 | true              | Reichert, Greenholt and Schoen | AEEX-CR5839-8192       | lobortis vel dapibus at diam nam tristique tortor                                                | true                | true                  | {"location":{"address1":"750 Continental Avenue","city":"Virginia Beach","country":"US","state":"Virginia","zip":"23459"}} |
      | 197    | add20781-c7c7-4b8a-8c71-a0819237674f | true              | Bosco, Buckridge and White     | AHZD-OA9907-6725       | ante vivamus tortor duis mattis egestas metus aenean fermentum                                   | false               | false                 | {"location":{"address1":"1 American Way","city":"Cincinnati","country":"US","state":"Ohio","zip":"45228"}}                 |
      | 32     | 19698339-5bc1-451a-be69-163f7692ec24 | true              | Roob Inc                       | AIRK-EZ2992-4313       | sagittis dui vel nisl duis ac nibh fusce lacus purus aliquet at                                  | false               | true                  | {"location":{"address1":"6651 Parkside Street","city":"Buffalo","country":"US","state":"New York","zip":"14205"}}          |
      | 106    | 8e239800-f7fa-4276-8cba-4e5e10bccfc7 | true              | Koch and Sons                  | AIUO-SZ7123-0254       | suscipit a feugiat et eros vestibulum ac est lacinia nisi venenatis tristique fusce              | false               | false                 | {"location":{"address1":"48 Comanche Hill","city":"Sioux Falls","country":"US","state":"South Dakota","zip":"57110"}}      |
      | 224    | e8ab786b-fe64-4cab-a710-fb8e0c1d18d1 | false             | Brown, Oberbrunner and Little  | AQGE-RN9319-9586       | lectus aliquam sit amet diam in magna bibendum imperdiet                                         | false               | false                 | {"location":{"address1":"6 Jana Circle","city":"Monticello","country":"US","state":"Minnesota","zip":"55590"}}             |
      | 198    | 9b1f3251-38f4-4b52-8ca6-be7917487df9 | true              | Murray, Renner and Gottlieb    | ARKR-UN3162-2974       | duis mattis egestas metus aenean fermentum donec ut mauris eget massa                            | true                | false                 | {"location":{"address1":"693 Waubesa Drive","city":"Waterbury","country":"US","state":"Connecticut","zip":"06705"}}        |
      | 212    | 5af9e1de-f91f-4645-a6a1-64cd9d2d9248 | false             | Leannon and Sons               | ATOS-PM9569-9254       | suspendisse accumsan tortor quis turpis sed ante vivamus tortor duis mattis egestas metus aenean | true                | false                 | {"location":{"address1":"355 La Follette Court","city":"Ridgely","country":"US","state":"Maryland","zip":"21684"}}         |
    Then Compare DataFrame MANUFACTURERS_JOIN_PRODUCTS from TRANSFORM order by product_id
      | manufacturer_id:string | manufacturer_name:string | product_id:string | product_name:string                     | is_available:boolean | is_discontinued:boolean | is_manufactured:boolean |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | COAV-RL0833-2046  | Wisozk-Lakin                            | true                 | true                    | true                    |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | DKEN-ZL7465-8683  | Bednar, Hills and Wyman                 | false                | false                   | false                   |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | DVPW-NS9444-6597  | Effertz, Beatty and Stiedemann          | false                | false                   | false                   |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | FEUI-PY5639-3856  | Schiller-Lebsack                        | true                 | true                    | false                   |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | IGPE-ID3036-4293  | Erdman Inc                              | false                | true                    | true                    |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | KIWC-DH8991-4690  | O'Hara Group                            | false                | true                    | true                    |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | KOFH-XA4567-1372  | White-Miller                            | true                 | true                    | true                    |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | NWDB-KA2482-4825  | Effertz, Kling and Brown                | false                | true                    | true                    |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | NWNG-FT8567-5880  | Cruickshank, Konopelski and Heidenreich | false                | false                   | false                   |
      | RRTL-MT6227-0318       | Steuber-Kiehn            | POMQ-EN3888-9662  | Rolfson-Zulauf                          | false                | false                   | true                    |

  Scenario: Filter manufactures by city and state, sort by zip
    When Run Bumblebee for 20171221
    And Filter MANUFACTURERS_LOCATIONS from TRANSFORM order by address1,zip
      | city:String | state:string |
      | Phoenix     | Arizona      |
      | Tucson      | Arizona      |
      | Escondido   | California   |
      | San Diego   | California   |
      | San Jose    | California   |
    Then Compare DataFrame MANUFACTURERS_FILTER_CITY_STATE from TRANSFORM order by address1,zip
      | address1:string      | city:string | state:string | zip:string | country:string |
      | 574 Oak Valley Drive | Phoenix     | Arizona      | 85072      | US             |
      | 9 Thompson Place     | Phoenix     | Arizona      | 85099      | US             |
      | 70278 Texas Road     | Tucson      | Arizona      | 85710      | US             |
      | 2557 Scoville Circle | Tucson      | Arizona      | 85715      | US             |
      | 54229 Maple Drive    | Tucson      | Arizona      | 85715      | US             |
      | 9 Northview Place    | Tucson      | Arizona      | 85715      | US             |
      | 59 Annamark Place    | Tucson      | Arizona      | 85748      | US             |
      | 47 Armistice Trail   | Escondido   | California   | 92030      | US             |
      | 37 Warner Road       | San Diego   | California   | 92105      | US             |
      | 2 Onsgard Street     | San Diego   | California   | 92115      | US             |
      | 33208 Clemons Place  | San Diego   | California   | 92132      | US             |
      | 803 Stephen Point    | San Diego   | California   | 92160      | US             |
      | 8 Burrows Alley      | San Jose    | California   | 95128      | US             |
      | 8099 Hoffman Trail   | San Jose    | California   | 95133      | US             |
      | 1626 Darwin Lane     | San Jose    | California   | 95173      | US             |

