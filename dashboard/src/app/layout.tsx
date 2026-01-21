import type { Metadata } from 'next';
import './globals.css';
import Link from 'next/link';
import styles from './layout.module.css';

export const metadata: Metadata = {
  title: 'Psychologie.ch Data Admin',
  description: 'Premium Data Maintenance Dashboard',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <div className={styles.layout}>
          <aside className={styles.sidebar}>
            <div className={styles.logo}>PsyAdmin</div>
            <nav className={styles.nav}>
              <Link href="/" className={styles.navItem}>Dashboard</Link>
              <Link href="/maintenance" className={styles.navItem}>Maintenance</Link>
              <Link href="/data" className={styles.navItem}>Data Browser</Link>
            </nav>
          </aside>
          <main className={styles.main}>
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}
