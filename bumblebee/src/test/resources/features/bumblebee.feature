@bumblebee-integration-test
Feature: Bumblebee Integration Test
  Test for ETL - Extract (read JSON), Transform (Transform DataFrames), Load (Dump data to persistence)

  Background: Mechanism to execute steps before all scenarios
    Given: Set environment variables
  Scenario: Run Bumblebee
    When Run Bumblebee for 20171221
    Then Compare DataFrame MANUFACTURERS from EXTRACT order by manufacturer_id
      | id:int | guid:string                          | is_active:boolean | name:string                    | manufacturer_id:string | description:string                                                                               | is_retailer:boolean | is_wholesaler:boolean |
      | 247    | 592f35ae-a541-42ba-a4ad-ad8831202636 | true              | Willms-O'Conner                | AAHH-KQ6566-2311       | dapibus augue vel accumsan tellus nisi eu orci mauris lacinia sapien quis                        | false               | true                  |
      | 199    | 2aa6368b-59a4-4782-8bab-59a2c3378f1d | false             | Ebert Group                    | ACPI-BM5744-4442       | erat fermentum justo nec condimentum neque sapien placerat ante nulla justo aliquam quis         | true                | false                 |
      | 107    | 3bbbd989-a9af-46e5-8453-eee1ea4ac664 | true              | Bauch-Bergnaum                 | ADNE-QL4757-5275       | donec ut mauris eget massa tempor convallis nulla neque libero convallis                         | false               | false                 |
      | 270    | 38c49062-3b25-407d-b1b1-74f5db46a837 | true              | Reichert, Greenholt and Schoen | AEEX-CR5839-8192       | lobortis vel dapibus at diam nam tristique tortor                                                | true                | true                  |
      | 197    | add20781-c7c7-4b8a-8c71-a0819237674f | true              | Bosco, Buckridge and White     | AHZD-OA9907-6725       | ante vivamus tortor duis mattis egestas metus aenean fermentum                                   | false               | false                 |
      | 32     | 19698339-5bc1-451a-be69-163f7692ec24 | true              | Roob Inc                       | AIRK-EZ2992-4313       | sagittis dui vel nisl duis ac nibh fusce lacus purus aliquet at                                  | false               | true                  |
      | 106    | 8e239800-f7fa-4276-8cba-4e5e10bccfc7 | true              | Koch and Sons                  | AIUO-SZ7123-0254       | suscipit a feugiat et eros vestibulum ac est lacinia nisi venenatis tristique fusce              | false               | false                 |
      | 224    | e8ab786b-fe64-4cab-a710-fb8e0c1d18d1 | false             | Brown, Oberbrunner and Little  | AQGE-RN9319-9586       | lectus aliquam sit amet diam in magna bibendum imperdiet                                         | false               | false                 |
      | 198    | 9b1f3251-38f4-4b52-8ca6-be7917487df9 | true              | Murray, Renner and Gottlieb    | ARKR-UN3162-2974       | duis mattis egestas metus aenean fermentum donec ut mauris eget massa                            | true                | false                 |
      | 212    | 5af9e1de-f91f-4645-a6a1-64cd9d2d9248 | false             | Leannon and Sons               | ATOS-PM9569-9254       | suspendisse accumsan tortor quis turpis sed ante vivamus tortor duis mattis egestas metus aenean | true                | false                 |
