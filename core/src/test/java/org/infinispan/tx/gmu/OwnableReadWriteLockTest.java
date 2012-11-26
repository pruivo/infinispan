package org.infinispan.tx.gmu;

import org.infinispan.util.concurrent.locks.OwnableReentrantReadWriteLock;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.OwnableReadWriteLockTest")
public class OwnableReadWriteLockTest {

   private static final long TIMEOUT = 10000;

   public void testMultipleRead() throws InterruptedException {
      OwnableReentrantReadWriteLock lock = newLock();
      Owner owner = owner(0);


      for (int i = 0; i < 100; i++) {
         lockShared(lock, owner, true);
      }

      assert lock.getLockState() == -100;

      for (int i = 0; i < 100; i++) {
         lock.unlock(owner);
      }

      assert lock.getLockState() == 0;
   }

   public void testReadWrite() throws InterruptedException {
      OwnableReentrantReadWriteLock lock = newLock();
      Owner owner = owner(0);

      lockShared(lock, owner, true);

      assert lock.getLockState() == -1;


      boolean result = lock.tryLock(owner, TIMEOUT, TimeUnit.MILLISECONDS);
      assert !result;

      assert lock.getLockState() == -1;

      lock.unlock(owner);

      assert lock.getLockState() == 0;
   }

   public void testWriteRead() throws InterruptedException {
      OwnableReentrantReadWriteLock lock = newLock();
      Owner owner = owner(0);

      lock.tryLock(owner, TIMEOUT, TimeUnit.MILLISECONDS);
      assert lock.getLockState() == 1;
      lockShared(lock, owner, true);
      assert lock.getLockState() == 1;
      lock.unlock(owner);
      assert lock.getLockState() == 0;
   }

   private Owner owner(int id) {
      return new Owner(id);
   }

   private OwnableReentrantReadWriteLock newLock() {
      return new OwnableReentrantReadWriteLock();
   }

   private void lockShared(OwnableReentrantReadWriteLock lock, Owner owner, boolean result) throws InterruptedException {
      boolean locked = lock.tryShareLock(owner, TIMEOUT, TimeUnit.MILLISECONDS);
      assertEquals(result, locked);
   }

   private static class Owner {
      private final int id;

      private Owner(int id) {
         this.id = id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Owner owner = (Owner) o;

         return id == owner.id;

      }

      @Override
      public int hashCode() {
         return id;
      }
   }

}
