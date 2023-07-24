package org.infinispan.multimap.impl;

import org.assertj.core.util.Lists;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.list;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_ARGS_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_ARGS_INDEXES_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_KEY_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_MEMBER_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_SCORES_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.MultimapTestUtils.CHARY;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELA;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.IGOR;
import static org.infinispan.multimap.impl.MultimapTestUtils.IZARO;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.SortedSetBucket.ScoredValue.of;
import static org.infinispan.multimap.impl.SortedSetSubsetArgs.create;

/**
 * Single Multimap Cache Test with Linked List
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "multimap.EmbeddedMultimapSortedSetCacheTest")
public class EmbeddedMultimapSortedSetCacheTest extends SingleCacheManagerTest {

   EmbeddedMultimapSortedSetCache<String, Person> sortedSetCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(MultimapSCI.INSTANCE);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      cm.createCache("test", builder.build());
      cm.getClassAllowList().addClasses(Person.class);
      sortedSetCache = new EmbeddedMultimapSortedSetCache<>(cm.getCache("test"));
      return cm;
   }

   public void validateSortedSetAddArgs() {
      SortedSetAddArgs emptyArgs = SortedSetAddArgs.create().build();
      assertThat(emptyArgs).isNotNull();
      assertThat(emptyArgs.addOnly).isFalse();
      assertThat(emptyArgs.updateOnly).isFalse();
      assertThat(emptyArgs.updateGreaterScoresOnly).isFalse();
      assertThat(emptyArgs.updateLessScoresOnly).isFalse();
      assertThat(emptyArgs.returnChangedCount).isFalse();

      // updateOnly and addOnly can't both be true
      assertThat(SortedSetAddArgs.create().addOnly().build().addOnly).isTrue();
      assertThat(SortedSetAddArgs.create().updateOnly().build().updateOnly).isTrue();
      assertThatThrownBy(() -> SortedSetAddArgs.create().addOnly().updateOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR);

      //  updateGt, updateLt and addOnly can't be all true
      assertThat(SortedSetAddArgs.create().updateGreaterScoresOnly().build().updateGreaterScoresOnly).isTrue();
      assertThat(SortedSetAddArgs.create().updateLessScoresOnly().build().updateLessScoresOnly).isTrue();
      assertThatThrownBy(() -> SortedSetAddArgs.create().addOnly().updateLessScoresOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);
      assertThatThrownBy(() -> SortedSetAddArgs.create().addOnly().updateGreaterScoresOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);
      assertThatThrownBy(() -> SortedSetAddArgs.create().updateLessScoresOnly().updateGreaterScoresOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);

   }

   public void testAddMany() {
      SortedSetAddArgs emptyArgs = SortedSetAddArgs.create().build();

      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(), emptyArgs))).isEqualTo(0);
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(2, OIHANA), of(4.2, ELAIA)), emptyArgs))).isEqualTo(2);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(2, OIHANA), of(4.2, ELAIA));
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(9, OIHANA)), emptyArgs))).isEqualTo(0);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(4.2, ELAIA), of(9, OIHANA) );
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(10, OIHANA), of(90, KOLDO)), emptyArgs))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(4.2, ELAIA), of(10, OIHANA), of(90, KOLDO));
      // count updates
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(7.9, ELAIA)),
            SortedSetAddArgs.create().returnChangedCount().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(7.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // add only
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(1, JULIEN), of(7.9, ELAIA)),
            SortedSetAddArgs.create().addOnly().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(1, JULIEN), of(7.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // update only (does not create)
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(2.2, RAMON), of(9.9, ELAIA)),
            SortedSetAddArgs.create().updateOnly().build()))).isEqualTo(0);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(1, JULIEN), of(9.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // update less than provided scores and create new ones
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(8.9, ELAIA), of(11.1, OIHANA), of(0.8, RAMON)),
            SortedSetAddArgs.create().updateLessScoresOnly().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(0.8, RAMON), of(1, JULIEN), of(8.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // update greater than provided scores and create new ones
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(6.9, ELAIA), of(12.1, OIHANA), of(32.98, FELIX)),
            SortedSetAddArgs.create().updateGreaterScoresOnly().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(0.8, RAMON), of(1, JULIEN), of(8.9, ELAIA), of(12.1, OIHANA), of(32.98, FELIX), of(90, KOLDO) );

      // add a new value with the same score as other
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, list(of(1, PEPE)), emptyArgs))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(0.8, RAMON), of(1, JULIEN),  of(1, PEPE), of(8.9, ELAIA), of(12.1, OIHANA), of(32.98, FELIX), of(90, KOLDO) );

      await(sortedSetCache.addMany(NAMES_KEY + "_lex",
            list(of(0, OIHANA), of(0, FELIX), of(0, KOLDO), of(0, ELA), of(0, ELAIA), of(0, RAMON)
            , of(0, IZARO), of(0, IGOR), of(0, CHARY), of(0, JULIEN)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSetCache.getValue(NAMES_KEY + "_lex")))
            .containsExactly(of(0, CHARY), of(0, ELA), of(0, ELAIA), of(0, FELIX), of(0, IGOR),
                  of(0, IZARO), of(0, JULIEN), of(0, KOLDO), of(0, OIHANA), of(0, RAMON));
      // Errors
      assertThatThrownBy(() -> await(sortedSetCache.addMany(null, null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(NAMES_KEY, null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_SCORES_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(NAMES_KEY, Lists.list(), null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ARGS_CAN_T_BE_NULL);
   }

   public void testCount() {
      assertThat(await(sortedSetCache.count(NAMES_KEY, 0, false, 0, false))).isEqualTo(0);
      await(sortedSetCache.addMany(NAMES_KEY,
            list(of(-5, OIHANA), of(1, ELAIA), of(2, KOLDO), of(5, RAMON),
                  of(8, JULIEN), of(8, FELIX),of(8, PEPE), of(9, IGOR), of(10, IZARO)),
            SortedSetAddArgs.create().build()));

      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, true, 8, true))).isEqualTo(3);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, false, 8, false))).isEqualTo(0);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, true, 8, false))).isEqualTo(0);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, false, 8, true))).isEqualTo(0);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, true, 9, true))).isEqualTo(7);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, true, 9, false))).isEqualTo(6);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, false, 9, false))).isEqualTo(5);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, false, 9, true))).isEqualTo(6);

      assertThatThrownBy(() -> await(sortedSetCache.count(null, 0, false, 0, false)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
   }

   public void testPop() {
      assertThat(await(sortedSetCache.pop(NAMES_KEY, false, 10))).isEmpty();
      await(sortedSetCache.addMany(NAMES_KEY,list(of(-5, OIHANA), of(1, ELAIA)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSetCache.pop(NAMES_KEY, true, 1))).containsExactly(of(-5, OIHANA));
      assertThat(await(sortedSetCache.pop(NAMES_KEY, true, 10))).containsExactly(of(1, ELAIA));
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).isNull();
      await(sortedSetCache.addMany(NAMES_KEY,
            list(of(-5, OIHANA), of(1, ELAIA), of(2, KOLDO), of(5, RAMON),
                  of(8, JULIEN), of(8, FELIX),of(8, PEPE), of(9, IGOR), of(10, IZARO)),
            SortedSetAddArgs.create().build()));
      assertThat(await(sortedSetCache.pop(NAMES_KEY, true, 2)))
            .containsExactly(of(-5, OIHANA), of(1, ELAIA));
      assertThat(await(sortedSetCache.pop(NAMES_KEY, false, 2)))
            .containsExactly(of(10, IZARO), of(9, IGOR));

      assertThatThrownBy(() -> await(sortedSetCache.pop(null, true, 1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
   }

   public void testScore() {
      assertThat(await(sortedSetCache.score(NAMES_KEY, OIHANA))).isNull();
      await(sortedSetCache.addMany(NAMES_KEY,
            list(of(-5, OIHANA), of(1, ELAIA)),
            SortedSetAddArgs.create().build()));
      assertThat(await(sortedSetCache.score(NAMES_KEY, OIHANA))).isEqualTo(-5);
      assertThat(await(sortedSetCache.score(NAMES_KEY, OIHANA))).isEqualTo(-5);
      assertThat(await(sortedSetCache.score(NAMES_KEY, FELIX))).isNull();
      assertThatThrownBy(() -> await(sortedSetCache.score(null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
      assertThatThrownBy(() -> await(sortedSetCache.score(NAMES_KEY, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_MEMBER_CAN_T_BE_NULL);
   }

   public void testSubsetByIndex() {
      SortedSetSubsetArgs.Builder<Long> args = create();
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(0L).stop(0L).isRev(false).build()))).isEmpty();
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(0L).stop(0L).isRev(true).build()))).isEmpty();
      await(sortedSetCache.addMany(NAMES_KEY,
            list(of(0, OIHANA), of(1, FELIX), of(2, KOLDO), of(4, ELA),
                  of(4, ELAIA), of(5, RAMON),of(6, IZARO), of(7, IGOR),
                  of(8, CHARY), of(8, JULIEN)),
            SortedSetAddArgs.create().build()));

      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(0L).stop(-1L).isRev(false).build())))
            .containsExactly(
                  of(0, OIHANA),
                  of(1, FELIX),
                  of(2, KOLDO),
                  of(4, ELA),
                  of(4, ELAIA),
                  of(5, RAMON),
                  of(6, IZARO),
                  of(7, IGOR),
                  of(8, CHARY),
                  of(8, JULIEN));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(0L).stop(-1L).isRev(true).build())))
            .containsExactly(
                  of(8, JULIEN),
                  of(8, CHARY),
                  of(7, IGOR),
                  of(6, IZARO),
                  of(5, RAMON),
                  of(4, ELAIA),
                  of(4, ELA),
                  of(2, KOLDO),
                  of(1, FELIX),
                  of(0, OIHANA));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(-100L).stop(100L).isRev(false).build()))).hasSize(10);
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(1L).stop(2L).isRev(false).build())))
            .containsExactly(of(1, FELIX), of(2, KOLDO));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(1L).stop(2L).isRev(true).build())))
            .containsExactly(of(8, CHARY), of(7, IGOR));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(0L).stop(0L).isRev(false).build())))
            .containsExactly(of(0, OIHANA));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(0L).stop(0L).isRev(true).build())))
            .containsExactly(of(8, JULIEN));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(-1L).stop(-1L).isRev(false).build())))
            .containsExactly(of(8, JULIEN));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(-1L).stop(-1L).isRev(true).build())))
            .containsExactly(of(0, OIHANA));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(3L).stop(3L).isRev(true).build())))
            .containsExactly(of(6, IZARO));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(3L).stop(3L).isRev(false).build())))
            .containsExactly(of(4, ELA));
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(3L).stop(3L).isRev(true).build())))
            .containsExactly(of(6, IZARO));

      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(-1L).stop(0L).isRev(false).build()))).isEmpty();
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(3L).stop(2L).isRev(false).build()))).isEmpty();
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(3L).stop(2L).isRev(true).build()))).isEmpty();
      assertThat(await(sortedSetCache.subsetByIndex(NAMES_KEY, args.start(-1L).stop(0L).isRev(true).build()))).isEmpty();

      // Errors
      assertThatThrownBy(() -> await(sortedSetCache.subsetByIndex(null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
      assertThatThrownBy(() -> await(sortedSetCache.subsetByIndex(NAMES_KEY, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ARGS_CAN_T_BE_NULL);
      assertThatThrownBy(() -> await(sortedSetCache.subsetByIndex(NAMES_KEY,  create().build())))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ARGS_INDEXES_CAN_T_BE_NULL);
      assertThatThrownBy(() -> await(sortedSetCache.subsetByIndex(NAMES_KEY,  create().start(12L).build())))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ARGS_INDEXES_CAN_T_BE_NULL);
   }

   public void testSubsetByScore() {
      SortedSetSubsetArgs.Builder<Double> args = create();
      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.isRev(false).build()))).isEmpty();
      await(sortedSetCache.addMany(NAMES_KEY,
            list(of(0, OIHANA), of(1, FELIX), of(2, KOLDO), of(4, ELA),
                  of(4, ELAIA), of(5, RAMON),of(6, IZARO), of(7, IGOR),
                  of(8, CHARY), of(8, JULIEN)),
            SortedSetAddArgs.create().build()));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.isRev(false).build())))
            .containsExactly(
                  of(0, OIHANA),
                  of(1, FELIX),
                  of(2, KOLDO),
                  of(4, ELA),
                  of(4, ELAIA),
                  of(5, RAMON),
                  of(6, IZARO),
                  of(7, IGOR),
                  of(8, CHARY),
                  of(8, JULIEN));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.isRev(true).build())))
            .containsExactly(
                  of(8, JULIEN),
                  of(8, CHARY),
                  of(7, IGOR),
                  of(6, IZARO),
                  of(5, RAMON),
                  of(4, ELAIA),
                  of(4, ELA),
                  of(2, KOLDO),
                  of(1, FELIX),
                  of(0, OIHANA));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(1d).stop(5d).isRev(false).build())))
            .containsExactly(
                  of(2, KOLDO),
                  of(4, ELA),
                  of(4, ELAIA));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(1d)
            .includeStart(true).stop(5d).includeStop(true).isRev(false).build())))
            .containsExactly(
                  of(1, FELIX),
                  of(2, KOLDO),
                  of(4, ELA),
                  of(4, ELAIA),
                  of(5, RAMON));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(1d)
            .includeStart(false).stop(5d).includeStop(true).isRev(false).build())))
            .containsExactly(
                  of(2, KOLDO),
                  of(4, ELA),
                  of(4, ELAIA),
                  of(5, RAMON));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(1d)
            .includeStart(true).stop(5d).includeStop(false).isRev(false).build())))
            .containsExactly(
                  of(1, FELIX),
                  of(2, KOLDO),
                  of(4, ELA),
                  of(4, ELAIA));


      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(5d)
            .includeStart(true).stop(1d).includeStop(true).isRev(true).build())))
            .containsExactly(
                  of(5, RAMON),
                  of(4, ELAIA),
                  of(4, ELA),
                  of(2, KOLDO),
                  of(1, FELIX));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(5d)
            .includeStart(false).stop(1d).includeStop(false).isRev(true).build())))
            .containsExactly(
                  of(4, ELAIA),
                  of(4, ELA),
                  of(2, KOLDO));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(5d)
            .includeStart(false).stop(1d).includeStop(true).isRev(true).build())))
            .containsExactly(
                  of(4, ELAIA),
                  of(4, ELA),
                  of(2, KOLDO),
                  of(1, FELIX));

      assertThat(await(sortedSetCache.subsetByScore(NAMES_KEY, args.start(5d)
            .includeStart(true).stop(1d).includeStop(false).isRev(true).build())))
            .containsExactly(
                  of(5, RAMON),
                  of(4, ELAIA),
                  of(4, ELA),
                  of(2, KOLDO));

      // Errors
      assertThatThrownBy(() -> await(sortedSetCache.subsetByScore(null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
      assertThatThrownBy(() -> await(sortedSetCache.subsetByScore(NAMES_KEY,  null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ARGS_CAN_T_BE_NULL);
   }

   public void testSubsetByLex() {
      SortedSetSubsetArgs.Builder<Person> args = create();
      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(false).build()))).isEmpty();
      await(sortedSetCache.addMany(NAMES_KEY,
            list(of(0, CHARY), of(0, ELA), of(0, ELAIA), of(0, FELIX),
                  of(0, IZARO), of(0, IGOR),of(0, JULIEN), of(0, KOLDO),
                  of(0, OIHANA), of(0, RAMON)),
            SortedSetAddArgs.create().build()));
      // unbounded
      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(false).build())))
            .containsExactly(
                  of(0, CHARY),
                  of(0, ELA),
                  of(0, ELAIA),
                  of(0, FELIX),
                  of(0, IGOR),
                  of(0, IZARO),
                  of(0, JULIEN),
                  of(0, KOLDO),
                  of(0, OIHANA),
                  of(0, RAMON));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(true).build())))
            .containsExactly(
                  of(0, RAMON),
                  of(0, OIHANA),
                  of(0, KOLDO),
                  of(0, JULIEN),
                  of(0, IZARO),
                  of(0, IGOR),
                  of(0, FELIX),
                  of(0, ELAIA),
                  of(0, ELA),
                  of(0, CHARY));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(false).start(JULIEN).includeStart(false).build())))
            .containsExactly(
                  of(0, KOLDO),
                  of(0, OIHANA),
                  of(0, RAMON));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(true).start(null).stop(JULIEN).includeStart(false).build())))
            .containsExactly(
                  of(0, RAMON),
                  of(0, OIHANA),
                  of(0, KOLDO));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(false).start(JULIEN).stop(null).includeStart(true).build())))
            .containsExactly(
                  of(0, JULIEN),
                  of(0, KOLDO),
                  of(0, OIHANA),
                  of(0, RAMON));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(false).start(null).stop(IGOR).includeStop(false).build())))
            .containsExactly(
                  of(0, CHARY),
                  of(0, ELA),
                  of(0, ELAIA),
                  of(0, FELIX));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(false)
            .start(null).stop(IGOR).includeStop(true).build())))
            .containsExactly(
                  of(0, CHARY),
                  of(0, ELA),
                  of(0, ELAIA),
                  of(0, FELIX),
                  of(0, IGOR));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.isRev(true)
            .start(IGOR).includeStart(true).stop(null).build())))
            .containsExactly(
                  of(0, IGOR),
                  of(0, FELIX),
                  of(0, ELAIA),
                  of(0, ELA),
                  of(0, CHARY));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.start(ELA).stop(FELIX).
            includeStop(true).includeStart(true).isRev(false).build())))
            .containsExactly(
                  of(0, ELA),
                  of(0, ELAIA),
                  of(0, FELIX));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.start(FELIX).stop(ELA).
            includeStop(true).includeStart(true).isRev(true).build())))
            .containsExactly(
                  of(0, FELIX),
                  of(0, ELAIA),
                  of(0, ELA));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.start(ELA).stop(FELIX).
            includeStop(false).includeStart(true).isRev(false).build())))
            .containsExactly(
                  of(0, ELA),
                  of(0, ELAIA));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.start(ELA).stop(FELIX).
            includeStop(true).includeStart(false).isRev(false).build())))
            .containsExactly(
                  of(0, ELAIA),
                  of(0, FELIX));

      assertThat(await(sortedSetCache.subsetByLex(NAMES_KEY, args.start(FELIX).stop(ELA).
            includeStop(true).includeStart(false).isRev(true).build())))
            .containsExactly(
                  of(0, ELAIA),
                  of(0, ELA));

      // Errors
      assertThatThrownBy(() -> await(sortedSetCache.subsetByLex(null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
      assertThatThrownBy(() -> await(sortedSetCache.subsetByLex(NAMES_KEY,  null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ARGS_CAN_T_BE_NULL);
   }
}
