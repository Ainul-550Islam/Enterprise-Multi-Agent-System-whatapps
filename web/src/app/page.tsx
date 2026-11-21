import { redirect } from "next/navigation";

/** Landing route: signed-in operators land on the overview dashboard. */
export default function DashboardPage() {
  redirect("/overview");
}
