# GameStatsHub - Analysis Of Gaming Profiles
<p align="center">
  <img src=./img/readme_logo.jpg />
</p>

![Steam](https://img.shields.io/badge/Steam-171d25)
![PlayStation](https://img.shields.io/badge/PlayStation-296cc8)
![Xbox](https://img.shields.io/badge/Xbox-107c10)

<h2 align="center">About</h2>
The goal of creating this dataset is to perform a comprehensive analysis and study of various aspects of gaming platforms, including user behavior and price dynamics. The project involves user segmentation, enabling the identification of key user groups and their preferences. This dataset is ideal for training in data analysis and the use of analytical tools such as statistical methods, machine learning, and data visualization. It aims to uncover hidden patterns and insights that can significantly assist indie developers and companies in making more informed decisions, improving user experience, and optimizing pricing strategies across different platforms

> <h3>Why Choose This Dataset?</h3>
<list>

* <b>Real User Data:</b> Based on real-world information for practical insights
* <b>Rich and Diverse Information (~60GB):</b> Offers a wide variety of data to explore
* <b>Regular Updates:</b> Ensures the dataset remains relevant and up-to-date
* <b>Tailored for Educational Projects:</b> Designed specifically for learning and project development
* <b>Documented and Open Source:</b> Fully documented and openly accessible for transparency and ease of use
</list>

> <h3>Sources</h3>
<list>

* <b>Steam + Steam Web API:</b> Comprehensive data on Steam games and users
* <b>Exophase:</b> Rankings and history of PlayStation and Xbox users
* <b>TrueAchievements:</b> General information about Xbox games
* <b>TrueTrophies:</b> General information about PlayStation games
* <b>PSPrices:</b> Daily price changes for PlayStation and Xbox games
</list>

> <h3>Coverage</h3>

<div style="display: flex; justify-content: center; width: 100%;">
  <table style="width: 95%; text-align: center; border-collapse: collapse;">
    <tr>
      <td style="border: 1px solid grey;"><b>Temporal Coverage Start Date</b></td>
      <td style="border: 1px solid grey;"><b>Temporal Coverage End Date</b></td>
      <td style="border: 1px solid grey;"><b>Geospatial Coverage</b></td>
    </tr>
    <tr>
      <td style="border: 1px solid grey;">12-09-2008</td>
      <td style="border: 1px solid grey;">12-01-2025</td>
      <td style="border: 1px solid grey;">World</td>
    </tr>
  </table>
</div>
<br>

> <b>If you found this dataset useful, please rate it on [Kaggle](https://www.kaggle.com/datasets/artyomkruglov/gaming-profiles-2025-steam-playstation-xbox)</b>

<h2 align="center">Install</h2>

    psql -f gamestatshub-YYYYMMDD.sql -U user

<h2 align="center">Data Pipeline</h2>
<p align="center">
  <img src=./img/data_pipeline.png />
</p>

<h2 align="center">ER Diagrams</h2>

<details>
  <summary>Click to see the Steam ERD</summary>
  <p align="center">
  <img src=./img/steam_erd.png />
  </p>
</details>
<br>
<details>  
  <summary>Click to see the PlayStation ERD</summary>
  <p align="center">
  <img src=./img/playstation_erd.png />
  </p>
</details>
<br>
<details>  
  <summary>Click to see the Xbox ERD</summary>
  <p align="center">
  <img src=./img/xbox_erd.png />
  </p>
</details>
