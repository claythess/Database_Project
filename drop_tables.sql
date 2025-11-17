SET FOREIGN_KEY_CHECKS = 0;
SELECT concat('DROP TABLE IF EXISTS `', table_name, '`;')
FROM information_schema.tables
WHERE table_schema = 'moviedb';

DROP TABLE IF EXISTS `crew`;
DROP TABLE IF EXISTS `production_company`;
DROP TABLE IF EXISTS `genre`;
DROP TABLE IF EXISTS `crew_movie`;
DROP TABLE IF EXISTS `favorite_director`;
DROP TABLE IF EXISTS `movie`;
DROP TABLE IF EXISTS `user_password`;
DROP TABLE IF EXISTS `movie_rating`;
DROP TABLE IF EXISTS `actor`;
DROP TABLE IF EXISTS `genre_movie`;
DROP TABLE IF EXISTS `director_movie`;
DROP TABLE IF EXISTS `user`;
DROP TABLE IF EXISTS `director`;
DROP TABLE IF EXISTS `follow`;
DROP TABLE IF EXISTS `favorite_actor`;
DROP TABLE IF EXISTS `production_company_movie`;
DROP TABLE IF EXISTS `session`;
DROP TABLE IF EXISTS `staging_imdb`;

SET FOREIGN_KEY_CHECKS = 1
